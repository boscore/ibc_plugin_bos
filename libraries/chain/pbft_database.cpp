#include <eosio/chain/pbft_database.hpp>
#include <fc/io/fstream.hpp>
#include <fstream>
#include <eosio/chain/global_property_object.hpp>
#include <random>

namespace eosio {
    namespace chain {

        pbft_database::pbft_database(controller& ctrl) :ctrl(ctrl) {
            checkpoint_index = pbft_checkpoint_state_multi_index_type();
            view_state_index = pbft_view_state_multi_index_type();
            prepare_watermarks = vector<block_num_type>{};
            pbft_db_dir = ctrl.state_dir();
            checkpoints_dir = ctrl.blocks_dir();
            chain_id = ctrl.get_chain_id();

            if (!fc::is_directory(pbft_db_dir)) fc::create_directories(pbft_db_dir);

            auto pbft_db_dat = pbft_db_dir / config::pbftdb_filename;
            if (fc::exists(pbft_db_dat)) {
                string content;
                fc::read_file_contents(pbft_db_dat, content);

                fc::datastream<const char *> ds(content.data(), content.size());

                //set current_view in pbftdb.dat.
                fc::raw::unpack(ds, _current_view);

                unsigned_int size;
                fc::raw::unpack(ds, size);
                for (uint32_t i = 0, n = size.value; i < n; ++i) {
                    pbft_state s;
                    fc::raw::unpack(ds, s);
                    set(std::make_shared<pbft_state>(move(s)));
                }
            } else {
                pbft_state_index = pbft_state_multi_index_type();
            }

            if (!fc::is_directory(checkpoints_dir)) fc::create_directories(checkpoints_dir);

            auto checkpoints_db = checkpoints_dir / config::checkpoints_filename;
            if (fc::exists(checkpoints_db)) {
                string content;
                fc::read_file_contents(checkpoints_db, content);

                fc::datastream<const char *> ds(content.data(), content.size());

                unsigned_int checkpoint_size;
                fc::raw::unpack(ds, checkpoint_size);
                for (uint32_t j = 0, m = checkpoint_size.value; j < m; ++j) {
                    pbft_checkpoint_state cs;
                    fc::raw::unpack(ds, cs);
                    set(std::make_shared<pbft_checkpoint_state>(move(cs)));
                }
                ilog("checkpoint index size: ${cs}", ("cs", checkpoint_index.size()));
            } else {
                checkpoint_index = pbft_checkpoint_state_multi_index_type();
            }

            fc::remove(checkpoints_db);
        }

        pbft_database::~pbft_database() {

            fc::path checkpoints_db = checkpoints_dir / config::checkpoints_filename;
            std::ofstream c_out(checkpoints_db.generic_string().c_str(),
                                std::ios::out | std::ios::binary | std::ofstream::trunc);

            uint32_t num_records_in_checkpoint_db = checkpoint_index.size();
            fc::raw::pack(c_out, unsigned_int{num_records_in_checkpoint_db});

            for (const auto& s: checkpoint_index) {
                fc::raw::pack(c_out, *s);
            }

            fc::path pbft_db_dat = pbft_db_dir / config::pbftdb_filename;
            std::ofstream out(pbft_db_dat.generic_string().c_str(),
                              std::ios::out | std::ios::binary | std::ofstream::trunc);
            fc::raw::pack(out, _current_view);

            uint32_t num_records_in_db = pbft_state_index.size();
            fc::raw::pack(out, unsigned_int{num_records_in_db});

            for (const auto& s : pbft_state_index) {
                fc::raw::pack(out, *s);
            }

            pbft_state_index.clear();
            checkpoint_index.clear();
        }

        void pbft_database::add_pbft_prepare(const pbft_prepare& p, const public_key_type& pk) {

            auto& by_block_id_index = pbft_state_index.get<by_block_id>();

            auto current = ctrl.fetch_block_state_by_id(p.block_info.block_id);

            while ((current) && (current->block_num > ctrl.last_irreversible_block_num())) {
                auto curr_itr = by_block_id_index.find(current->id);

                if (curr_itr == by_block_id_index.end()) {
                    try {
                        flat_map<std::pair<pbft_view_type, public_key_type>, pbft_prepare> prepares;
                        prepares[std::make_pair(p.view, pk)] = p;
                        pbft_state curr_ps;
                        curr_ps.block_id = current->id;
                        curr_ps.block_num = current->block_num;
                        curr_ps.prepares = prepares;
                        auto curr_psp = std::make_shared<pbft_state>(move(curr_ps));
                        pbft_state_index.insert(curr_psp);
                    } catch (...) {
                        elog( "prepare insert failure: ${p}", ("p", p));
                    }
                } else {
                    auto prepares = (*curr_itr)->prepares;
                    if (prepares.find(std::make_pair(p.view, pk)) == prepares.end()) {
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr& psp) {
                            psp->prepares[std::make_pair(p.view, pk)] = p;
                        });
                    } else {
                        return;
                    }
                }
                curr_itr = by_block_id_index.find(current->id);
                if (curr_itr == by_block_id_index.end()) return;

                auto cpsp = *curr_itr;
                auto prepares = cpsp->prepares;
                auto as = current->active_schedule.producers;
                auto threshold = as.size()* 2 / 3 + 1;
                if (prepares.size() >= threshold && !cpsp->is_prepared && is_less_than_high_watermark(cpsp->block_num)) {
                    flat_map<pbft_view_type, uint32_t> prepare_count;
                    for (const auto& pre: prepares) {
                        if (prepare_count.find(pre.second.view) == prepare_count.end()) prepare_count[pre.second.view] = 0;
                    }

                    for (const auto& bp: as) {
                        for (const auto& pp: prepares) {
                            if (bp.block_signing_key == pp.first.second) prepare_count[pp.first.first] += 1;
                        }
                    }
                    for (const auto& e: prepare_count) {
                        if (e.second >= threshold) {
                            mark_as_prepared(cpsp->block_id);
                        }
                    }
                }
                current = ctrl.fetch_block_state_by_id(current->prev());
            }
        }

        void pbft_database::mark_as_prepared(const block_id_type& bid) {
            auto& by_block_id_index = pbft_state_index.get<by_block_id>();
            auto itr = by_block_id_index.find(bid);
            auto bnum = block_info_type{bid}.block_num();

            if (itr == by_block_id_index.end()) {
                pbft_state ps;
                ps.block_id = bid;
                ps.block_num = bnum;
                ps.is_prepared = true;
                auto psp = std::make_shared<pbft_state>(move(ps));
                pbft_state_index.insert(psp);
                return;
            }
            by_block_id_index.modify(itr, [&](const pbft_state_ptr& p) { p->is_prepared = true; });
        }

        vector<pbft_prepare> pbft_database::generate_and_add_pbft_prepare(const pbft_prepare& cached_prepare) {
            vector<pbft_prepare> prepares_to_be_cached;
            const auto& my_sps = ctrl.my_signature_providers();
            prepares_to_be_cached.reserve(my_sps.size());
            auto head_block_num = ctrl.head_block_num();
            if (head_block_num <= 1) return prepares_to_be_cached;
            auto my_prepare = ctrl.get_pbft_my_prepare();

            auto reserve_prepare = [&](const block_id_type& in) {
                if (in == block_id_type()) return false;
                auto bs = ctrl.fetch_block_state_by_id(in);
                if (!bs) return false;
                auto lib = ctrl.last_irreversible_block_id();
                if (lib == block_id_type()) return true;
                auto forks = ctrl.fork_db().fetch_branch_from(in, lib);

                //`branch_type` will always contain at least themselves.
                //`in` block num should be higher than lib, yet fall on the same branch with lib.
                return forks.first.size() > 1
                && forks.second.size() == 1
                && !bs->in_current_chain;
            };


            if (!cached_prepare.empty()) {
                for (const auto& sp : my_sps) {
                    //sign again, update cache, then emit
                    auto retry_p = cached_prepare;
                    retry_p.common.timestamp = time_point::now();
                    retry_p.sender_signature = sp.second(retry_p.digest(chain_id));
                    if (is_valid_prepare(retry_p, sp.first)) {
                        prepares_to_be_cached.emplace_back(retry_p);
                    }
                }
            } else if (reserve_prepare(my_prepare) ) {
                for (const auto& sp : my_sps) {
                    pbft_prepare reserve_p;
                    reserve_p.view = _current_view;
                    reserve_p.block_info = {my_prepare};
                    reserve_p.sender_signature = sp.second(reserve_p.digest(chain_id));
                    if (is_valid_prepare(reserve_p, sp.first)) {
                        prepares_to_be_cached.emplace_back(reserve_p);
                    }
                }
            } else {
                auto current_watermark = get_current_pbft_watermark();
                auto lib = ctrl.last_irreversible_block_num();

                uint32_t high_watermark_block_num = head_block_num;

                if ( current_watermark > 0 ) {
                    high_watermark_block_num = std::min(head_block_num, current_watermark);
                }

                if (high_watermark_block_num <= lib) return prepares_to_be_cached;

                auto hwbs = ctrl.fork_db().get_block_in_current_chain_by_num(high_watermark_block_num);
                if ( hwbs && hwbs->id != my_prepare) {
                    auto sent = false;
                    for (const auto& sp : my_sps) {
                        pbft_prepare new_p;
                        new_p.view = _current_view;
                        new_p.block_info = {hwbs->id};
                        new_p.sender_signature = sp.second(new_p.digest(chain_id));
                        if (is_valid_prepare(new_p, sp.first)) {
                            add_pbft_prepare(new_p, sp.first);
                            sent = true;
                            prepares_to_be_cached.emplace_back(new_p);
                        }
                    }
                    if (sent) ctrl.set_pbft_my_prepare(hwbs->id);
                }
            }
            return prepares_to_be_cached;
        }

        bool pbft_database::should_prepared() {

            const auto& by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
            auto itr = by_prepare_and_num_index.begin();
            if (itr == by_prepare_and_num_index.end()) return false;

            pbft_state_ptr psp = *itr;

            if (psp->is_prepared && (psp->block_num > ctrl.last_irreversible_block_num())) {
                ctrl.set_pbft_prepared((*itr)->block_id);
                return true;
            }
            return false;
        }

        bool pbft_database::is_valid_prepare(const pbft_prepare& p, const public_key_type& pk) {
            // a prepare msg under lscb (which is no longer in fork_db), can be treated as null, thus true.
            if (p.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            return should_recv_pbft_msg(pk);
        }

        void pbft_database::add_pbft_commit(const pbft_commit& c, const public_key_type& pk) {

            auto& by_block_id_index = pbft_state_index.get<by_block_id>();

            auto current = ctrl.fetch_block_state_by_id(c.block_info.block_id);

            while ((current) && (current->block_num > ctrl.last_irreversible_block_num())) {

                auto curr_itr = by_block_id_index.find(current->id);

                if (curr_itr == by_block_id_index.end()) {
                    try {
                        flat_map<std::pair<pbft_view_type, public_key_type>, pbft_commit> commits;
                        commits[std::make_pair(c.view, pk)] = c;
                        pbft_state curr_ps;
                        curr_ps.block_id = current->id;
                        curr_ps.block_num = current->block_num;
                        curr_ps.commits = commits;
                        auto curr_psp = std::make_shared<pbft_state>(move(curr_ps));
                        pbft_state_index.insert(curr_psp);
                    } catch (...) {
                        elog("commit insertion failure: ${c}", ("c", c));
                    }
                } else {
                    auto commits = (*curr_itr)->commits;
                    if (commits.find(std::make_pair(c.view, pk)) == commits.end()) {
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr& psp) {
                            psp->commits[std::make_pair(c.view, pk)] = c;
                            std::sort(psp->commits.begin(), psp->commits.end(), less<>());
                        });
                    } else {
                        return;
                    }
                }

                curr_itr = by_block_id_index.find(current->id);
                if (curr_itr == by_block_id_index.end()) return;

                auto cpsp = *curr_itr;

                auto as = current->active_schedule.producers;
                auto threshold = as.size()* 2 / 3 + 1;
                auto commits = cpsp->commits;
                if (commits.size() >= threshold && !cpsp->is_committed && is_less_than_high_watermark(cpsp->block_num)) {
                    flat_map<pbft_view_type, uint32_t> commit_count;
                    for (const auto& com: commits) {
                        if (commit_count.find(com.second.view) == commit_count.end()) commit_count[com.second.view] = 0;
                    }

                    for (const auto& bp: as) {
                        for (const auto& pc: commits) {
                            if (bp.block_signing_key == pc.first.second) commit_count[pc.first.first] += 1;
                        }
                    }

                    for (const auto& e: commit_count) {
                        if (e.second >= threshold) {
                            mark_as_committed(cpsp->block_id);
                        }
                    }
                }
                current = ctrl.fetch_block_state_by_id(current->prev());
            }
        }

        vector<pbft_commit> pbft_database::generate_and_add_pbft_commit(const pbft_commit& cached_commit) {
            vector<pbft_commit> commits_to_be_cached;
            const auto& my_sps = ctrl.my_signature_providers();
            commits_to_be_cached.reserve(my_sps.size());

            if (!cached_commit.empty()) {
                for (const auto& sp : my_sps) {
                    //sign again, update cache, then emit
                    auto retry_c = cached_commit;
                    retry_c.common.timestamp = time_point::now();
                    retry_c.sender_signature = sp.second(retry_c.digest(chain_id));
                    if (is_valid_commit(retry_c, sp.first)) {
                        commits_to_be_cached.emplace_back(retry_c);
                    }
                }
            } else {
                const auto& by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
                auto itr = by_prepare_and_num_index.begin();
                if (itr == by_prepare_and_num_index.end()) return commits_to_be_cached;

                pbft_state_ptr psp = *itr;
                auto bs = ctrl.fork_db().get_block(psp->block_id);
                if (!bs) return commits_to_be_cached;

                if (psp->is_prepared && (psp->block_num > ctrl.last_irreversible_block_num())) {

                    for (const auto& sp : my_sps) {
                        pbft_commit new_c;
                        new_c.view = _current_view;
                        new_c.block_info = {psp->block_id};
                        new_c.sender_signature = sp.second(new_c.digest(chain_id));

                        if (is_valid_commit(new_c, sp.first)) {
                            add_pbft_commit(new_c, sp.first);
                            commits_to_be_cached.emplace_back(new_c);
                        }
                    }
                }
            }
            return commits_to_be_cached;
        }

        void  pbft_database::mark_as_committed(const block_id_type& bid) {
            auto& by_block_id_index = pbft_state_index.get<by_block_id>();
            auto itr = by_block_id_index.find(bid);
            if (itr == by_block_id_index.end()) return;
            by_block_id_index.modify(itr, [&](const pbft_state_ptr& p) { p->is_committed = true; });
        }

        bool pbft_database::should_committed() {
            const auto& by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return false;
            pbft_state_ptr psp = *itr;

            return (psp->is_committed && (psp->block_num > ctrl.last_irreversible_block_num()));
        }

        pbft_view_type pbft_database::get_committed_view() {
            pbft_view_type new_view = 0;
            if (!should_committed()) return new_view;

            const auto& by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            pbft_state_ptr psp = *itr;

            auto blk_state = ctrl.fetch_block_state_by_id((*itr)->block_id);
            if (!blk_state) return new_view;
            auto as = blk_state->active_schedule.producers;

            auto commits = (*itr)->commits;

            auto threshold = as.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> commit_count;
            for (const auto& com: commits) {
                if (commit_count.find(com.second.view) == commit_count.end()) commit_count[com.second.view] = 0;
            }

            for (const auto& bp: as) {
                for (const auto& pc: commits) {
                    if (bp.block_signing_key == pc.first.second) commit_count[pc.first.first] += 1;
                }
            }

            for (const auto& e: commit_count) {
                if (e.second >= threshold && e.first > new_view) {
                    new_view = e.first;
                }
            }
            return new_view;
        }

        bool pbft_database::is_valid_commit(const pbft_commit& c, const public_key_type& pk) {
            if (c.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            return should_recv_pbft_msg(pk);
        }

        void pbft_database::commit_local() {
            const auto& by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return;

            pbft_state_ptr psp = *itr;

            ctrl.pbft_commit_local(psp->block_id);
        }

        bool pbft_database::pending_pbft_lib() {
            return ctrl.pending_pbft_lib();
        }

        void pbft_database::add_pbft_view_change(const pbft_view_change& vc, const public_key_type& pk) {

            auto lscb_bps = lscb_active_producers().producers;

            auto& by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(vc.target_view);
            if (itr == by_view_index.end()) {
                flat_map<public_key_type, pbft_view_change> view_changes;
                view_changes[pk] = vc;
                pbft_view_change_state vcs;
                vcs.view = vc.target_view;
                vcs.view_changes = view_changes;
                auto vcsp = std::make_shared<pbft_view_change_state>(move(vcs));
                view_state_index.insert(vcsp);
            } else {
                auto pvs = (*itr);
                auto view_changes = pvs->view_changes;

                if (view_changes.find(pk) == view_changes.end()) {
                    by_view_index.modify(itr, [&](const pbft_view_change_state_ptr& pvsp) {
                        pvsp->view_changes[pk] = vc;
                    });
                } else {
                    return;
                }
            }

            itr = by_view_index.find(vc.target_view);
            if (itr == by_view_index.end()) return;

            auto vsp = *itr;
            auto threshold = lscb_bps.size() * 2 / 3 + 1;
            if (vsp->view_changes.size() >= threshold && !vsp->is_view_changed) {
                auto vc_count = 0;

                for (const auto& bp: lscb_bps) {
                    for (const auto& v: vsp->view_changes) {
                        if (bp.block_signing_key == v.first) vc_count += 1;
                    }
                }
                if (vc_count >= threshold) {
                    by_view_index.modify(itr, [&](const pbft_view_change_state_ptr& pvsp) { pvsp->is_view_changed = true; });
                }
            }
        }

        pbft_view_type pbft_database::should_view_change() {
            pbft_view_type nv = 0;
            auto& by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.begin();
            if (itr == by_view_index.end()) return nv;

            while (itr != by_view_index.end()) {
                auto active_bps = lscb_active_producers().producers;
                auto vc_count = 0;
                auto pvs = (*itr);

                for (const auto& bp: active_bps) {
                    for (const auto& v: pvs->view_changes) {
                        if (bp.block_signing_key == v.first) vc_count += 1;
                    }
                }
                //if contains self or view_change >= f+1, transit to view_change and send view change
                if (vc_count > (active_bps.size() - 1) / 3) {
                    nv = pvs->view;
                    break;
                }
                ++itr;
            }
            return nv;
        }

        vector<pbft_view_change> pbft_database::generate_and_add_pbft_view_change(
                const pbft_view_change& cached_view_change,
                const pbft_prepared_certificate& ppc,
                const vector<pbft_committed_certificate>& pcc,
                pbft_view_type target_view) {

            vector<pbft_view_change> view_changes_to_be_cached;
            const auto& my_sps = ctrl.my_signature_providers();
            view_changes_to_be_cached.reserve(my_sps.size());

            if (!cached_view_change.empty()) {
                for (const auto& sp : my_sps) {
                    //sign again, update cache, then emit
                    auto retry_vc = cached_view_change;
                    retry_vc.common.timestamp = time_point::now();
                    retry_vc.sender_signature = sp.second(retry_vc.digest(chain_id));
                    if (is_valid_view_change(retry_vc, sp.first)) {
                        view_changes_to_be_cached.emplace_back(retry_vc);
                    }
                }
            } else {
                for (const auto& sp : my_sps) {
                    auto my_lsc = get_stable_checkpoint_by_id(ctrl.last_stable_checkpoint_block_id());

                    pbft_view_change new_vc;
                    new_vc.current_view = _current_view;
                    new_vc.target_view = target_view;
                    new_vc.prepared_cert = ppc;
                    new_vc.committed_certs = pcc;
                    new_vc.stable_checkpoint = my_lsc;
                    new_vc.sender_signature = sp.second(new_vc.digest(chain_id));
                    if (is_valid_view_change(new_vc, sp.first)) {
                        add_pbft_view_change(new_vc, sp.first);
                        view_changes_to_be_cached.emplace_back(new_vc);
                    }
                }
            }
            return view_changes_to_be_cached;
        }

        bool pbft_database::should_new_view(pbft_view_type target_view) {
            auto& by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(target_view);
            if (itr == by_view_index.end()) return false;
            return (*itr)->is_view_changed;
        }

        pbft_view_type pbft_database::get_proposed_new_view_num() {
            auto& by_count_and_view_index = view_state_index.get<by_count_and_view>();
            auto itr = by_count_and_view_index.begin();
            if (itr == by_count_and_view_index.end() || !(*itr)->is_view_changed) return 0;
            return (*itr)->view;
        }

        bool pbft_database::has_new_primary(const public_key_type& pk) {

            if (pk == public_key_type()) return false;
            auto sps = ctrl.my_signature_providers();
            auto sp_itr = sps.find(pk);
            return sp_itr != sps.end();
        }

        void pbft_database::cleanup_on_new_view() {
            view_state_index.clear();
            ctrl.reset_pbft_my_prepare();
        }

        pbft_new_view pbft_database::generate_pbft_new_view(
                const pbft_view_changed_certificate& vcc,
                pbft_view_type new_view) {

            pbft_new_view nv;

            auto primary_key = get_new_view_primary_key(new_view);
            if (!has_new_primary(primary_key) || vcc.empty()) return nv;

            //`sp_itr` is not possible to be the end iterator, since it's already been checked in `has_new_primary`.
            auto my_sps = ctrl.my_signature_providers();
            auto sp_itr = my_sps.find(primary_key);

            auto highest_ppc = pbft_prepared_certificate();
            auto highest_pcc = vector<pbft_committed_certificate>{};
            auto highest_sc = pbft_stable_checkpoint();

            // shuffle view changes to avoid fixed orders.
            std::vector<int> idx_v(vcc.view_changes.size()) ;
            std::iota (std::begin(idx_v), std::end(idx_v), 0);
            auto seed = std::chrono::system_clock::now().time_since_epoch().count();
            std::shuffle( idx_v.begin(), idx_v.end(), std::default_random_engine(seed));
            for (const auto i: idx_v) {
                auto& prepared_cert = vcc.view_changes[i].prepared_cert;
                auto& committed_certs = vcc.view_changes[i].committed_certs;
                auto& stable_ckpts = vcc.view_changes[i].stable_checkpoint;
                if (prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()
                && is_valid_prepared_certificate(prepared_cert)) {
                    highest_ppc = prepared_cert;
                }

                for (const auto& cc:committed_certs ) {
                    auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                            [&](const pbft_committed_certificate& ext) { return ext.block_info.block_id == cc.block_info.block_id; });
                    if (p_itr == highest_pcc.end() && is_valid_committed_certificate(cc)) highest_pcc.emplace_back(cc);
                }

                if (stable_ckpts.block_info.block_num() > highest_sc.block_info.block_num()
                && is_valid_stable_checkpoint(stable_ckpts)) {
                    highest_sc = stable_ckpts;
                }
            }

            nv.new_view = new_view;
            nv.prepared_cert = highest_ppc;
            nv.committed_certs = highest_pcc;
            nv.stable_checkpoint = highest_sc;
            nv.view_changed_cert = vcc;
            nv.sender_signature = sp_itr->second(nv.digest(chain_id));
            try {
                validate_new_view(nv, sp_itr->first);
            } catch (const fc::exception& ex) {
                elog("bad new view, ${s} ", ("s", ex.to_string()));
                nv = pbft_new_view();
            }
            return nv;
        }

        pbft_prepared_certificate pbft_database::generate_prepared_certificate() {

            pbft_prepared_certificate ppc;

            const auto& by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
            auto itr = by_prepare_and_num_index.begin();
            if (itr == by_prepare_and_num_index.end()) return ppc;
            pbft_state_ptr psp = *itr;

            auto prepared_block_state = ctrl.fetch_block_state_by_id(psp->block_id);
            if (!prepared_block_state) return ppc;

            auto as = prepared_block_state->active_schedule.producers;
            if (psp->is_prepared && psp->block_num > ctrl.last_irreversible_block_num()) {
                auto prepares = psp->prepares;
                auto valid_prepares = vector<pbft_prepare>{};

                flat_map<pbft_view_type, uint32_t> prepare_count;
                flat_map<pbft_view_type, vector<pbft_prepare>> prepare_msg;

                for (const auto& pre: prepares) {
                    if (prepare_count.find(pre.first.first) == prepare_count.end()) prepare_count[pre.first.first] = 0;
                }

                for (const auto& bp: as) {
                    for (const auto& pp: prepares) {
                        if (bp.block_signing_key == pp.first.second) {
                            prepare_count[pp.first.first] += 1;
                            prepare_msg[pp.first.first].emplace_back(pp.second);
                        }
                    }
                }

                auto bp_threshold = as.size() * 2 / 3 + 1;
                for (const auto& e: prepare_count) {
                    if (e.second >= bp_threshold) {
                        valid_prepares = prepare_msg[e.first];
                    }
                }

                if (valid_prepares.empty()) return ppc;

                ppc.block_info = {psp->block_id};
                ppc.prepares=valid_prepares;
                ppc.pre_prepares.emplace(psp->block_id);
                for (const auto& p: valid_prepares) {
                    auto bid = p.block_info.block_id;
                    while (bid != psp->block_id) {
                        ppc.pre_prepares.emplace(bid);
                        bid = ctrl.fetch_block_state_by_id(bid)->prev();
                    }
                }
            }
            return ppc;
        }

        vector<pbft_committed_certificate> pbft_database::generate_committed_certificate() {

            vector<pbft_committed_certificate> pcc{};

            const auto& by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return pcc;

            pbft_state_ptr psp = *itr;

            if (!psp->is_committed) return pcc;

            auto highest_committed_block_num = psp->block_num;

            vector<block_num_type> ccb;

            auto lib_num = ctrl.last_irreversible_block_num();

            //adding my highest committed cert.
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            if ( highest_committed_block_num <= lib_num && highest_committed_block_num > lscb_num ) {
                ccb.emplace_back(highest_committed_block_num);
            }

            auto watermarks = get_updated_watermarks();
            for (auto& watermark : watermarks) {
                //adding committed cert on every water mark.
                if (watermark < lib_num && watermark > lscb_num) {
                    ccb.emplace_back(watermark);
                }
            }

            const auto& by_id_index = pbft_state_index.get<by_block_id>();

            std::sort(ccb.begin(), ccb.end());
            pcc.reserve(ccb.size());
            for (const auto& committed_block_num: ccb) {
                auto cbs = ctrl.fetch_block_state_by_number(committed_block_num);
                if (!cbs) return pcc;

                auto it = by_id_index.find(cbs->id);
                if (it == by_id_index.end() || !(*it)->is_committed) {
                    return pcc;
                }

                auto as = cbs->active_schedule.producers;

                auto commits = (*it)->commits;
                auto valid_commits = vector<pbft_commit>{};

                flat_map<pbft_view_type, uint32_t> commit_count;
                flat_map<pbft_view_type, vector<pbft_commit>> commit_msg;

                for (const auto& com: commits) {
                    if (commit_count.find(com.first.first) == commit_count.end()) commit_count[com.first.first] = 0;
                }

                for (const auto& bp: as) {
                    for (const auto& cc: commits) {
                        if (bp.block_signing_key == cc.first.second) {
                            commit_count[cc.first.first] += 1;
                            commit_msg[cc.first.first].emplace_back(cc.second);
                        }
                    }
                }

                auto bp_threshold = as.size() * 2 / 3 + 1;
                for (const auto& e: commit_count) {
                    if (e.second >= bp_threshold) {
                        valid_commits = commit_msg[e.first];
                    }
                }

                if (valid_commits.empty()) return pcc;

                pbft_committed_certificate cc;
                cc.block_info = {cbs->id};
                cc.commits = valid_commits;
                pcc.emplace_back(cc);
            }
            return pcc;
        }

        pbft_view_changed_certificate pbft_database::generate_view_changed_certificate(pbft_view_type target_view) {

            pbft_view_changed_certificate pvcc;

            auto& by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(target_view);
            if (itr == by_view_index.end()) return pvcc;

            auto pvs = *itr;

            auto lscb_bps = lscb_active_producers().producers;

            if (pvs->is_view_changed) {
                pvcc.target_view=pvs->view;
                pvcc.view_changes.reserve(pvs->view_changes.size());
                for (auto& bp : lscb_bps) {
                    for(auto& view_change : pvs->view_changes) {
                        if (bp.block_signing_key == view_change.first) pvcc.view_changes.emplace_back(view_change.second);
                    }
                }
            }
            return pvcc;
        }



        bool pbft_database::is_valid_prepared_certificate(const pbft_prepared_certificate& certificate, bool add_to_pbft_db) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lib is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_irreversible_block_num()) return true;

            auto prepares = certificate.prepares;
            auto prepares_metadata = vector<pbft_message_metadata<pbft_prepare>>{};
            prepares_metadata.reserve(prepares.size());

            for (auto& p : prepares) {
                auto pmm = pbft_message_metadata<pbft_prepare>(p, chain_id);
                prepares_metadata.emplace_back(pmm);
                if ( add_to_pbft_db && is_valid_prepare(p, pmm.sender_key) ) add_pbft_prepare(p, pmm.sender_key);
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> prepare_count;
            flat_map<pbft_view_type, vector<producer_and_block_info>> prepare_msg;

            for (const auto& pm: prepares_metadata) {
                if (prepare_count.find(pm.msg.view) == prepare_count.end()) {
                    prepare_count[pm.msg.view] = 0;
                    prepare_msg[pm.msg.view].reserve(bp_threshold);
                }
            }

            for (const auto& bp: producer_schedule.producers) {
                for (const auto& pm: prepares_metadata) {
                    if (bp.block_signing_key == pm.sender_key) {
                        prepare_count[pm.msg.view] += 1;
                        prepare_msg[pm.msg.view].emplace_back(std::make_pair(pm.sender_key, pm.msg.block_info));
                    }
                }
            }

            auto should_prepared = false;
            auto valid_prepares = vector<producer_and_block_info>{};
            valid_prepares.reserve(bp_threshold);
            for (const auto& e: prepare_count) {
                if (e.second >= bp_threshold) {
                    should_prepared = true;
                    valid_prepares = prepare_msg[e.first];
                }
            }

            return should_prepared && is_valid_longest_fork(valid_prepares, certificate.block_info, true);
        }

        bool pbft_database::is_valid_committed_certificate(const pbft_committed_certificate& certificate, bool add_to_pbft_db, bool at_the_top) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lscb (no longer in fork_db) is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;

            auto commits = certificate.commits;
            auto commits_metadata = vector<pbft_message_metadata<pbft_commit>>{};
            commits_metadata.reserve(commits.size());

            for (auto& c : commits) {
                auto pmm = pbft_message_metadata<pbft_commit>(c, chain_id);
                commits_metadata.emplace_back(pmm);
                if (add_to_pbft_db && is_valid_commit(c, pmm.sender_key)) add_pbft_commit(c, pmm.sender_key);
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> commit_count;
            flat_map<pbft_view_type, vector<producer_and_block_info>> commit_msg;

            for (const auto& cm: commits_metadata) {
                if (commit_count.find(cm.msg.view) == commit_count.end()) {
                    commit_count[cm.msg.view] = 0;
                    commit_msg[cm.msg.view].reserve(bp_threshold);
                }
            }

            for (const auto& bp: producer_schedule.producers) {
                for (const auto& cm: commits_metadata) {
                    if (bp.block_signing_key == cm.sender_key) {
                        commit_count[cm.msg.view] += 1;
                        commit_msg[cm.msg.view].emplace_back(std::make_pair(cm.sender_key, cm.msg.block_info));
                    }
                }
            }

            auto should_committed = false;
            auto valid_commits = vector<producer_and_block_info>{};
            valid_commits.reserve(bp_threshold);
            for (const auto& e: commit_count) {
                if (e.second >= bp_threshold) {
                    should_committed = true;
                    valid_commits = commit_msg[e.first];
                }
            }

            return should_committed && is_valid_longest_fork(valid_commits, certificate.block_info, at_the_top);
        }

        bool pbft_database::is_valid_longest_fork(const vector<producer_and_block_info>& block_infos, const block_info_type& cert_info, bool at_the_top ) {

            //add all valid block_infos in to a temp multi_index, this implementation might contains heavier computation
            auto local_index = local_state_multi_index_type();
            auto& by_block_id_index = local_index.get<by_block_id>();

            auto next_watermark = get_current_pbft_watermark();

            for (auto &e: block_infos) {

                auto current = ctrl.fetch_block_state_by_id(e.second.block_id);

                while ( current ) {
                    if (next_watermark == 0 || current->block_num <= next_watermark) {

                        auto curr_itr = by_block_id_index.find(current->id);

                        if (curr_itr == by_block_id_index.end()) {
                            try {
                                vector<public_key_type> keys;
                                keys.reserve(block_infos.size());
                                keys.emplace_back(e.first);
                                validation_state curr_ps;
                                curr_ps.block_id = current->id;
                                curr_ps.block_num = current->block_num;
                                curr_ps.producers = keys;
                                auto curr_psp = std::make_shared<validation_state>(move(curr_ps));
                                local_index.insert(curr_psp);
                            } catch (...) {}
                        } else {
                            auto keys = (*curr_itr)->producers;
                            if (std::find(keys.begin(), keys.end(), e.first) == keys.end()) {
                                by_block_id_index.modify(curr_itr, [&](const validation_state_ptr &vsp) {
                                    vsp->producers.emplace_back(e.first);
                                });
                            }
                        }

                        curr_itr = by_block_id_index.find(current->id);
                        if (curr_itr != by_block_id_index.end()) {

                            auto cpsp = *curr_itr;

                            auto as = current->active_schedule.producers;
                            auto threshold = as.size() * 2 / 3 + 1;
                            auto keys = cpsp->producers;
                            if (keys.size() >= threshold && !cpsp->enough) {
                                uint32_t count = 0;

                                for (const auto &bp: as) {
                                    for (const auto &k: keys) {
                                        if (bp.block_signing_key == k) count += 1;
                                    }
                                }

                                if (count >= threshold) {
                                    by_block_id_index.modify(curr_itr,
                                            [&](const validation_state_ptr &p) { p->enough = true; });
                                }
                            }
                        }
                    }
                    current = ctrl.fetch_block_state_by_id(current->prev());
                }
            }

            if (!at_the_top) {
                auto itr = by_block_id_index.find(cert_info.block_id);
                if (itr == by_block_id_index.end()) return false;
                return (*itr)->enough;

            } else {
                const auto &by_status_and_num_index = local_index.get<by_status_and_num>();
                auto itr = by_status_and_num_index.begin();
                if (itr == by_status_and_num_index.end()) return false;

                validation_state_ptr psp = *itr;
                return psp->enough && psp->block_id == cert_info.block_id;
            }
        }

        bool pbft_database::is_valid_view_change(const pbft_view_change& vc, const public_key_type& pk) {

            return should_recv_pbft_msg(pk);
            // No need to check prepared cert and stable checkpoint, until generate or validate a new view msg
        }

        void pbft_database::validate_new_view(const pbft_new_view& nv, const public_key_type& pk) {

            EOS_ASSERT(pk == get_new_view_primary_key(nv.new_view), pbft_exception,
                       "new view is not signed with expected key");

            EOS_ASSERT(is_valid_prepared_certificate(nv.prepared_cert, true), pbft_exception,
                       "bad prepared certificate: ${pc}", ("pc", nv.prepared_cert));

            EOS_ASSERT(is_valid_stable_checkpoint(nv.stable_checkpoint, true), pbft_exception,
                       "bad stable checkpoint: ${scp}", ("scp", nv.stable_checkpoint));

            auto committed_certs = nv.committed_certs;
            std::sort(committed_certs.begin(), committed_certs.end());
            for (const auto& c: committed_certs) {
                EOS_ASSERT(is_valid_committed_certificate(c, true), pbft_exception,
                           "bad committed certificate: ${cc}", ("cc", c));
            }

            EOS_ASSERT(nv.view_changed_cert.target_view == nv.new_view, pbft_exception, "target view not match");

            vector<public_key_type> lscb_producers;
            lscb_producers.reserve(lscb_active_producers().producers.size());
            for (const auto& bp: lscb_active_producers().producers) {
                lscb_producers.emplace_back(bp.block_signing_key);
            }
            auto schedule_threshold = lscb_producers.size() * 2 / 3 + 1;

            auto view_changes = nv.view_changed_cert.view_changes;
            auto view_changes_metadata = vector<pbft_message_metadata<pbft_view_change>>{};
            view_changes_metadata.reserve(view_changes.size());

            vector<public_key_type> view_change_producers;
            view_change_producers.reserve(view_changes.size());
            for (auto& vc : view_changes) {
                auto pmm = pbft_message_metadata<pbft_view_change>(vc, chain_id);
                view_changes_metadata.emplace_back(pmm);
                if (is_valid_view_change(vc, pmm.sender_key)) {
                    add_pbft_view_change(vc, pmm.sender_key);
                    view_change_producers.emplace_back(pmm.sender_key);
                }
            }

            vector<public_key_type> intersection;

            std::sort(lscb_producers.begin(), lscb_producers.end());
            std::sort(view_change_producers.begin(), view_change_producers.end());
            std::set_intersection(lscb_producers.begin(), lscb_producers.end(),
                                  view_change_producers.begin(), view_change_producers.end(),
                                  back_inserter(intersection));

            EOS_ASSERT(intersection.size() >= schedule_threshold, pbft_exception, "view changes count not enough");

            EOS_ASSERT(should_new_view(nv.new_view), pbft_exception, "should not enter new view: ${nv}",
                       ("nv", nv.new_view));

            auto highest_ppc = pbft_prepared_certificate();
            auto highest_pcc = vector<pbft_committed_certificate>{};
            auto highest_scp = pbft_stable_checkpoint();

            for (const auto& vc: nv.view_changed_cert.view_changes) {
                if (vc.prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()
                    && is_valid_prepared_certificate(vc.prepared_cert)) {
                    highest_ppc = vc.prepared_cert;
                }

                for (const auto& cc: vc.committed_certs) {
                    if (is_valid_committed_certificate(cc)) {
                        auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                                             [&](const pbft_committed_certificate& ext) {
                                                 return ext.block_info.block_id == cc.block_info.block_id;
                                             });
                        if (p_itr == highest_pcc.end()) highest_pcc.emplace_back(cc);
                    }
                }

                if (vc.stable_checkpoint.block_info.block_num() > highest_scp.block_info.block_num()
                    && is_valid_stable_checkpoint(vc.stable_checkpoint, true)) {
                    highest_scp = vc.stable_checkpoint;
                }
            }

            EOS_ASSERT(highest_ppc.block_info == nv.prepared_cert.block_info, pbft_exception,
                       "prepared certificate does not match, should be ${hppc} but ${pc} given",
                       ("hppc", highest_ppc)("pc", nv.prepared_cert));

            std::sort(highest_pcc.begin(), highest_pcc.end());
            EOS_ASSERT(highest_pcc.size() == committed_certs.size(), pbft_exception,
                       "wrong committed certificates size");
            for (auto i = 0; i < committed_certs.size(); ++i) {
                EOS_ASSERT(highest_pcc[i].block_info == committed_certs[i].block_info, pbft_exception,
                           "committed certificate does not match, should be ${hpcc} but ${cc} given",
                           ("hpcc", highest_pcc[i])("cc", committed_certs[i]));
            }

            if (!committed_certs.empty()) {
                EOS_ASSERT(is_valid_committed_certificate(committed_certs.back(), false, true), pbft_exception,
                           "highest committed certificate is invalid, ${cc}", 
                           ("cc", committed_certs.back()));
            }

            EOS_ASSERT(highest_scp.block_info == nv.stable_checkpoint.block_info, pbft_exception,
                       "stable checkpoint does not match, should be ${hscp} but ${scp} given",
                       ("hpcc", highest_scp)("pc", nv.stable_checkpoint));
        }

        bool pbft_database::should_stop_view_change(const pbft_view_change& vc) {
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto vc_lscb = vc.stable_checkpoint.block_info.block_num();
            return vc_lscb > 0 && lscb_num > vc_lscb;
        }

        pbft_stable_checkpoint pbft_database::fetch_stable_checkpoint_from_blk_extn(const signed_block_ptr& b) {

            pbft_stable_checkpoint psc;
            try {
                if (b) {
                    auto& extn = b->block_extensions;

                    for (auto it = extn.begin(); it != extn.end();) {
                        if (it->first == static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint)) {
                            auto scp_ds = it->second;
                            fc::datastream<char *> ds(scp_ds.data(), scp_ds.size());
                            fc::raw::unpack(ds, psc);

                            if (is_valid_stable_checkpoint(psc)) {
                                break;
                            } else {
                                it = extn.erase(it);
                            }
                        } else {
                            it++;
                        }
                    }
                }
            } catch(...) {
                elog("no stable checkpoints found in the block extension");
                psc = pbft_stable_checkpoint();
            }
            return psc;
        }

        pbft_stable_checkpoint pbft_database::get_stable_checkpoint_by_id(const block_id_type& block_id, bool incl_blk_extn ) {
            pbft_stable_checkpoint psc;
            const auto& by_block = checkpoint_index.get<by_block_id>();
            auto itr = by_block.find(block_id);
            if (itr == by_block.end()) {
                if (incl_blk_extn) {
                    auto blk = ctrl.fetch_block_by_id(block_id);
                    psc = fetch_stable_checkpoint_from_blk_extn(blk);
                }
                return psc;
            }

            auto cpp = *itr;

            if (cpp->is_stable) {
                psc.block_info={cpp->block_id};
                psc.checkpoints.reserve(cpp->checkpoints.size());
                for (auto& checkpoint : cpp->checkpoints) {
                    psc.checkpoints.emplace_back(checkpoint.second) ;
                }

            }
            return psc;
        }

        block_info_type pbft_database::cal_pending_stable_checkpoint() const {

            auto pending_scb_num = ctrl.last_stable_checkpoint_block_num();
            auto pending_scb_info = block_info_type{ctrl.last_stable_checkpoint_block_id()};

            const auto& by_blk_num = checkpoint_index.get<by_num>();
            auto itr = by_blk_num.lower_bound(pending_scb_num);
            if (itr == by_blk_num.end()) return pending_scb_info;

            while (itr != by_blk_num.end()) {
                if (auto bs = ctrl.fetch_block_state_by_id((*itr)->block_id)) {
                    auto scb = ctrl.fetch_block_state_by_number(pending_scb_num);

                    auto head_checkpoint_schedule = bs->active_schedule;

                    producer_schedule_type current_schedule;
                    producer_schedule_type new_schedule;

                    if (pending_scb_num == 0) {
                        const auto& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                        if (ucb == 0) {
                            current_schedule = ctrl.initial_schedule();
                            new_schedule = ctrl.initial_schedule();
                        } else {
                            auto ucb_state = ctrl.fetch_block_state_by_number(ucb);
                            if (!ucb_state) return pending_scb_info;
                            current_schedule = ucb_state->active_schedule;
                            new_schedule = ucb_state->pending_schedule;
                        }
                    } else if (scb) {
                        current_schedule = scb->active_schedule;
                        new_schedule = scb->pending_schedule;
                    } else {
                        return pending_scb_info;
                    }

                    if ((*itr)->is_stable) {
                        if (head_checkpoint_schedule == current_schedule || head_checkpoint_schedule == new_schedule) {
                            pending_scb_info = block_info_type{(*itr)->block_id};
                            pending_scb_num = pending_scb_info.block_num();
                        } else {
                            return pending_scb_info;
                        }
                    }
                }
                ++itr;
            }
            return pending_scb_info;
        }

        vector<pbft_checkpoint> pbft_database::generate_and_add_pbft_checkpoint() {
            auto checkpoint = [&](const block_num_type& in) {
                auto const& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                if (!ctrl.is_pbft_enabled()) return false;
                if (in <= ucb) return false;
                auto watermarks = get_updated_watermarks();
                return in == ucb + 1 // checkpoint on first pbft block;
                       || in % get_checkpoint_interval() == 1 // checkpoint on every 100 block;
                       || std::find(watermarks.begin(), watermarks.end(), in) != watermarks.end(); // checkpoint on bp schedule change;
            };

            vector<pbft_checkpoint> new_pc{};
            auto my_sps = ctrl.my_signature_providers();

            const auto& by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end() || !(*itr)->is_committed) return new_pc;

            pbft_state_ptr psp = (*itr);
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();

            vector<block_num_type> pending_checkpoint_block_num;
            pending_checkpoint_block_num.reserve(psp->block_num - lscb_num);
            for (auto i = psp->block_num; i > lscb_num && i > 1; --i) {
                if (checkpoint(i)) {
                    pending_checkpoint_block_num.emplace_back(i);
                }
            }
            auto& by_block = checkpoint_index.get<by_block_id>();

            if (!pending_checkpoint_block_num.empty()) {
                std::sort(pending_checkpoint_block_num.begin(), pending_checkpoint_block_num.end());
                for (auto bnum: pending_checkpoint_block_num) {
                    if (auto bs = ctrl.fork_db().get_block_in_current_chain_by_num(bnum)) {
                        for (const auto& sp : my_sps) {
                            pbft_checkpoint cp;
                            cp.block_info = {bs->id};
                            cp.sender_signature = sp.second(cp.digest(chain_id));
                            if (is_valid_checkpoint(cp, sp.first)) {
                                add_pbft_checkpoint(cp, sp.first);
                                new_pc.emplace_back(cp);
                            }
                        }
                    }
                }
            } else if (lscb_num > 0) { //retry sending my lscb
                for (const auto& sp : my_sps) {
                    pbft_checkpoint cp;
                    cp.block_info={ctrl.last_stable_checkpoint_block_id()};
                    cp.sender_signature = sp.second(cp.digest(chain_id));
                    if (is_valid_checkpoint(cp, sp.first)) {
                        new_pc.emplace_back(cp);
                    }
                }
            }
            return new_pc;
        }

        void pbft_database::add_pbft_checkpoint(const pbft_checkpoint& cp, const public_key_type& pk) {

            auto cp_block_state = ctrl.fetch_block_state_by_id(cp.block_info.block_id);
            if (!cp_block_state) return;

            auto& by_block = checkpoint_index.get<by_block_id>();
            auto itr = by_block.find(cp.block_info.block_id);
            if (itr == by_block.end()) {
                flat_map<public_key_type, pbft_checkpoint> checkpoints;
                checkpoints[pk] = cp;
                pbft_checkpoint_state cs;
                cs.block_id = cp.block_info.block_id;
                cs.block_num = cp.block_info.block_num();
                cs.checkpoints = checkpoints;
                auto csp = std::make_shared<pbft_checkpoint_state>(move(cs));
                checkpoint_index.insert(csp);
                itr = by_block.find(cp.block_info.block_id);
            } else {
                auto csp = (*itr);
                auto checkpoints = csp->checkpoints;
                if (checkpoints.find(pk) == checkpoints.end()) {
                    by_block.modify(itr, [&](const pbft_checkpoint_state_ptr& pcp) {
                        csp->checkpoints[pk] = cp;
                    });
                } else {
                    return;
                }
            }

            auto csp = (*itr);
            auto active_bps = cp_block_state->active_schedule.producers;
            auto threshold = active_bps.size() * 2 / 3 + 1;
            if (csp->checkpoints.size() >= threshold && !csp->is_stable) {
                auto cp_count = 0;

                for (const auto& bp: active_bps) {
                    for (const auto& c: csp->checkpoints) {
                        if (bp.block_signing_key == c.first) cp_count += 1;
                    }
                }
                if (cp_count >= threshold) {
                    by_block.modify(itr, [&](const pbft_checkpoint_state_ptr& pcp) { csp->is_stable = true; });
                    auto id = csp->block_id;
                    auto blk = ctrl.fetch_block_by_id(id);

                    if (blk && (blk->block_extensions.empty() || blk->block_extensions.back().first != static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint))) {
                        auto scp = get_stable_checkpoint_by_id(id);
                        auto scp_size = fc::raw::pack_size(scp);

                        auto buffer = std::make_shared<vector<char>>(scp_size);
                        fc::datastream<char*> ds( buffer->data(), scp_size);
                        fc::raw::pack( ds, scp );

                        blk->block_extensions.emplace_back();
                        auto& extension = blk->block_extensions.back();
                        extension.first = static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint );
                        extension.second.resize(scp_size);
                        std::copy(buffer->begin(),buffer->end(), extension.second.data());
                    }
                }
            }
        }

        void pbft_database::checkpoint_local() {
            auto pending_scb_info = cal_pending_stable_checkpoint();
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto pending_num = pending_scb_info.block_num();
            auto pending_id = pending_scb_info.block_id;
            if (pending_num > lscb_num) {
                ctrl.set_pbft_latest_checkpoint(pending_id);
                if (ctrl.last_irreversible_block_num() < pending_num) ctrl.pbft_commit_local(pending_id);
                auto& by_block_id_index = pbft_state_index.get<by_block_id>();
                auto pitr = by_block_id_index.find(pending_id);
                if (pitr != by_block_id_index.end()) {
                    prune(*pitr);
                }
            }
            auto& bni = checkpoint_index.get<by_num>();
            auto oldest = bni.begin();
            while ( oldest != bni.end()
                 && (*oldest)->is_stable
                 && (*oldest)->block_num < lscb_num - oldest_stable_checkpoint ) {
                prune(*oldest);
                oldest = bni.begin();
            }
        }

        bool pbft_database::is_valid_checkpoint(const pbft_checkpoint& cp, const public_key_type& pk) {

            if (cp.block_info.block_num() > ctrl.head_block_num() || cp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return false;

            if (auto bs = ctrl.fetch_block_state_by_id(cp.block_info.block_id)) {
                auto active_bps = bs->active_schedule.producers;
                for (const auto& bp: active_bps) {
                    if (bp.block_signing_key == pk) return true;
                }
            }
            return false;
        }

        bool pbft_database::is_valid_stable_checkpoint(const pbft_stable_checkpoint& scp, bool add_to_pbft_db) {
            if (scp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num())
                // the stable checkpoint is way behind lib, no way getting the block state,
                // it will not be applied nor saved, thus considered safe.
                return true;

            auto checkpoints = scp.checkpoints;
            auto checkpoints_metadata = vector<pbft_message_metadata<pbft_checkpoint>>{};
            checkpoints_metadata.reserve(checkpoints.size());

            for (auto& cp : checkpoints) {
                auto pmm = pbft_message_metadata<pbft_checkpoint>(cp, chain_id);
                checkpoints_metadata.emplace_back(pmm);
                if (cp.block_info != scp.block_info || !is_valid_checkpoint(cp, pmm.sender_key)) return false;
                if (add_to_pbft_db) add_pbft_checkpoint(cp, pmm.sender_key);
            }

            if (add_to_pbft_db) checkpoint_local();


            if (auto bs = ctrl.fetch_block_state_by_number(scp.block_info.block_num())) {
                auto as = bs->active_schedule;
                auto cp_count = 0;
                for (const auto& bp: as.producers) {
                    for (const auto& cpm: checkpoints_metadata) {
                        if (bp.block_signing_key == cpm.sender_key) cp_count += 1;
                    }
                }
                return cp_count >= as.producers.size() * 2 / 3 + 1;
            }
            return false;

        }

        bool pbft_database::should_send_pbft_msg() {

            auto schedules = get_updated_fork_schedules();
            for (const auto& bp: schedules) {
                for (const auto& sp: ctrl.my_signature_providers()) {
                    if (bp.first == sp.first) return true;
                }
            }
            return false;
        }

        bool pbft_database::should_recv_pbft_msg(const public_key_type& pub_key) {

            auto schedules = get_updated_fork_schedules();
            for (const auto& bp: schedules) {
                if (bp.first == pub_key) return true;
            }
            return false;
        }

        public_key_type pbft_database::get_new_view_primary_key(pbft_view_type target_view) {

            auto active_bps = lscb_active_producers().producers;
            if (active_bps.empty()) return public_key_type();

            return active_bps[target_view % active_bps.size()].block_signing_key;
        }

        producer_schedule_type pbft_database::lscb_active_producers() const {

            auto ps = ctrl.initial_schedule();
            auto num = ctrl.last_stable_checkpoint_block_num();

            if (num == 0) {
                const auto& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                if (ucb == 0) return ps;
                num = ucb;
            }

            if (auto bs = ctrl.fetch_block_state_by_number(num)) {
                if (bs->pending_schedule.producers.empty()) {
                    ps = bs->active_schedule;
                } else {
                    ps = bs->pending_schedule;
                }
            }
            return ps;
        }

        block_num_type pbft_database::get_current_pbft_watermark() {
            auto lib = ctrl.last_irreversible_block_num();

            auto watermarks = get_updated_watermarks();
            if (watermarks.empty()) return 0;

            auto cw = std::upper_bound(watermarks.begin(), watermarks.end(), lib);

            if (cw == watermarks.end() || *cw <= lib) return 0;

            return *cw;
        }

        void pbft_database::update_fork_schedules() {

            auto vector_minus = [&](vector<block_num_type>& v1, vector<block_num_type>& v2)
            {
                vector<block_num_type> diff;
                std::set_difference(v1.begin(), v1.end(), v2.begin(), v2.end(),
                                    std::inserter(diff, diff.begin()));
                return diff;
            };

            auto watermarks = ctrl.get_watermarks();

            if (watermarks != prepare_watermarks) {
                auto prev = prepare_watermarks;
                prepare_watermarks = watermarks;
                std::sort(prepare_watermarks.begin(), prepare_watermarks.end());
                auto added = vector_minus(prepare_watermarks, prev);
                auto removed = vector_minus(prev, prepare_watermarks);
                for (auto i: added) {
                    if (auto bs = ctrl.fetch_block_state_by_number(i)) {
                        auto as = bs->active_schedule.producers;
                        for (const auto& bp: as) {
                            auto key = bp.block_signing_key;
                            if (fork_schedules.find(key) == fork_schedules.end()) {
                                fork_schedules[key] = i;
                            } else if ( i > fork_schedules[key]) {
                                fork_schedules[key] = i;
                            }
                        }
                    }
                }
                if (!removed.empty()) {
                    auto removed_num = *max_element(removed.begin(), removed.end());
                    for (auto itr = fork_schedules.begin(); itr != fork_schedules.end();) {
                        if ((*itr).second <= removed_num) {
                            itr = fork_schedules.erase(itr);
                        } else {
                            ++itr;
                        }
                    }
                }
            }

            auto lscb_bps = lscb_active_producers().producers;
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            for (const auto& bp: lscb_bps) {
                if (fork_schedules.find(bp.block_signing_key) == fork_schedules.end()
                || fork_schedules[bp.block_signing_key] < lscb_num) {
                    fork_schedules[bp.block_signing_key] = lscb_num;
                }
            }
        }

        vector<block_num_type>& pbft_database::get_updated_watermarks() {
            update_fork_schedules();
            return prepare_watermarks;
        }

        flat_map<public_key_type, uint32_t>& pbft_database::get_updated_fork_schedules() {
            update_fork_schedules();
            return fork_schedules;
        }

        uint16_t pbft_database::get_view_change_timeout() const {
            return ctrl.get_pbft_properties().configuration.view_change_timeout;
        }

        uint16_t pbft_database::get_checkpoint_interval() const {
            return ctrl.get_pbft_properties().configuration.pbft_checkpoint_granularity;
        }

        bool pbft_database::is_less_than_high_watermark(block_num_type bnum) {
            auto current_watermark = get_current_pbft_watermark();
            return current_watermark == 0 || bnum <= current_watermark;
        }

        pbft_state_ptr pbft_database::get_pbft_state_by_id(const block_id_type& id) const {

            auto& by_block_id_index = pbft_state_index.get<by_block_id>();
            auto itr = by_block_id_index.find(id);

            if (itr != by_block_id_index.end()) return (*itr);

            return pbft_state_ptr();
        }

        vector<pbft_checkpoint_state> pbft_database::get_checkpoints_by_num(block_num_type num) const {
            auto results = vector<pbft_checkpoint_state>{};
            auto& by_num_index = checkpoint_index.get<by_num>();

            auto pitr  = by_num_index.lower_bound( num );
            while(pitr != by_num_index.end() && (*pitr)->block_num == num ) {
                results.emplace_back(*(*pitr));
                ++pitr;
            }

            return results;
        }

        pbft_view_change_state_ptr pbft_database::get_view_changes_by_target_view(pbft_view_type tv) const {
            auto& by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(tv);

            if (itr != by_view_index.end()) return (*itr);

            return pbft_view_change_state_ptr();
        }

        vector<block_num_type> pbft_database::get_pbft_watermarks() const {
            return prepare_watermarks;
        }

        flat_map<public_key_type, uint32_t> pbft_database::get_pbft_fork_schedules() const {
            return fork_schedules;
        }

        void pbft_database::set(const pbft_state_ptr& s) {
            auto result = pbft_state_index.insert(s);

            EOS_ASSERT(result.second, pbft_exception, "unable to insert pbft state, duplicate state detected");
        }

        void pbft_database::set(const pbft_checkpoint_state_ptr& s) {
            auto result = checkpoint_index.insert(s);

            EOS_ASSERT(result.second, pbft_exception, "unable to insert pbft checkpoint index, duplicate state detected");
        }

        void pbft_database::prune(const pbft_state_ptr& h) {
            auto num = h->block_num;

            auto& by_bn = pbft_state_index.get<by_num>();
            auto bni = by_bn.begin();
            while (bni != by_bn.end() && (*bni)->block_num < num) {
                prune(*bni);
                bni = by_bn.begin();
            }

            auto itr = pbft_state_index.find(h->block_id);
            if (itr != pbft_state_index.end()) {
                pbft_state_index.erase(itr);
            }
        }

        void pbft_database::prune(const pbft_checkpoint_state_ptr& h) {
            auto num = h->block_num;

            auto& by_bn = checkpoint_index.get<by_num>();
            auto bni = by_bn.begin();
            while (bni != by_bn.end() && (*bni)->block_num < num) {
                prune(*bni);
                bni = by_bn.begin();
            }

            auto itr = checkpoint_index.find(h->block_id);
            if (itr != checkpoint_index.end()) {
                checkpoint_index.erase(itr);
            }
        }
    }
}