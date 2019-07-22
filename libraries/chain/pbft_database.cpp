#include <eosio/chain/pbft_database.hpp>
#include <fc/io/fstream.hpp>
#include <fstream>
#include <eosio/chain/global_property_object.hpp>

namespace eosio {
    namespace chain {

        pbft_database::pbft_database(controller &ctrl) :
                ctrl(ctrl) {
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

                //skip current_view in pbftdb.dat.
                ds.seekp(ds.tellp() + 4);

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

        void pbft_database::close() {

            fc::path checkpoints_db = checkpoints_dir / config::checkpoints_filename;
            std::ofstream c_out(checkpoints_db.generic_string().c_str(),
                                std::ios::out | std::ios::binary | std::ofstream::trunc);

            uint32_t num_records_in_checkpoint_db = checkpoint_index.size();
            fc::raw::pack(c_out, unsigned_int{num_records_in_checkpoint_db});

            for (auto const &s: checkpoint_index) {
                fc::raw::pack(c_out, *s);
            }

            fc::path pbft_db_dat = pbft_db_dir / config::pbftdb_filename;
            std::ofstream out(pbft_db_dat.generic_string().c_str(),
                              std::ios::out | std::ios::binary | std::ofstream::app);
            uint32_t num_records_in_db = pbft_state_index.size();
            fc::raw::pack(out, unsigned_int{num_records_in_db});

            for (auto const &s : pbft_state_index) {
                fc::raw::pack(out, *s);
            }

            pbft_state_index.clear();
            checkpoint_index.clear();
        }

        pbft_database::~pbft_database() {
            close();
        }

        void pbft_database::add_pbft_prepare(pbft_prepare &p, const public_key_type &pk) {

            auto &by_block_id_index = pbft_state_index.get<by_block_id>();

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
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr &psp) {
                            psp->prepares[std::make_pair(p.view, pk)] = p;
                        });
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
                    for (auto const &pre: prepares) {
                        if (prepare_count.find(pre.second.view) == prepare_count.end()) prepare_count[pre.second.view] = 0;
                    }

                    for (auto const &bp: as) {
                        for (auto const &pp: prepares) {
                            if (bp.block_signing_key == pp.first.second) prepare_count[pp.first.first] += 1;
                        }
                    }
                    for (auto const &e: prepare_count) {
                        if (e.second >= threshold) {
                            mark_as_prepared(cpsp->block_id);
                        }
                    }
                }
                current = ctrl.fetch_block_state_by_id(current->prev());
            }
        }

        void pbft_database::mark_as_prepared(const block_id_type &bid) {
            auto &by_block_id_index = pbft_state_index.get<by_block_id>();
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
            by_block_id_index.modify(itr, [&](const pbft_state_ptr &p) { p->is_prepared = true; });
        }

        pbft_prepare pbft_database::send_and_add_pbft_prepare(const pbft_prepare &cached_prepare, pbft_view_type current_view) {
            auto prepare_to_be_cached = pbft_prepare();

            auto head_block_num = ctrl.head_block_num();
            if (head_block_num <= 1) return prepare_to_be_cached;
            auto my_prepare = ctrl.get_pbft_my_prepare();

            auto reserve_prepare = [&](const block_id_type &in) {
                if (in == block_id_type() || !ctrl.fetch_block_state_by_id(in)) return false;
                auto lib = ctrl.last_irreversible_block_id();
                if (lib == block_id_type()) return true;
                auto forks = ctrl.fork_db().fetch_branch_from(in, lib);

                //`branch_type` will always contain at least themselves.
                //`in` block num should be higher than lib, yet fall on the same branch with lib.
                return forks.first.size() > 1 && forks.second.size() == 1;
            };


            if (!cached_prepare.empty()) {
                for (auto const &sp : ctrl.my_signature_providers()) {
                    //sign again, update cache, then emit
                    auto retry_p = cached_prepare;
                    retry_p.common.timestamp = time_point::now();
                    retry_p.sender_signature = sp.second(retry_p.digest(chain_id));
                    emit(pbft_outgoing_prepare, std::make_shared<pbft_prepare>(retry_p));
                }
                return prepare_to_be_cached;
            } else if (reserve_prepare(my_prepare)) {
                for (auto const &sp : ctrl.my_signature_providers()) {
                    pbft_prepare reserve_p;
                    reserve_p.view=current_view; reserve_p.block_info={my_prepare};
                    reserve_p.sender_signature = sp.second(reserve_p.digest(chain_id));
                    emit(pbft_outgoing_prepare, std::make_shared<pbft_prepare>(reserve_p));
                    if (prepare_to_be_cached.empty()) prepare_to_be_cached = reserve_p;
                }
                return prepare_to_be_cached;
            } else {

                auto current_watermark = get_current_pbft_watermark();
                auto lib = ctrl.last_irreversible_block_num();

                uint32_t high_watermark_block_num = head_block_num;

                if ( current_watermark > 0 ) {
                    high_watermark_block_num = std::min(head_block_num, current_watermark);
                }

                if (high_watermark_block_num <= lib) return prepare_to_be_cached;

                if (auto hwbs = ctrl.fork_db().get_block_in_current_chain_by_num(high_watermark_block_num)) {
                    auto sent = false;
                    for (auto const &sp : ctrl.my_signature_providers()) {
                        pbft_prepare new_p;
                        new_p.view=current_view; new_p.block_info={hwbs->id};
                        new_p.sender_signature = sp.second(new_p.digest(chain_id));
                        if (is_valid_prepare(new_p, sp.first)) {
                            emit(pbft_outgoing_prepare, std::make_shared<pbft_prepare>(new_p));
                            add_pbft_prepare(new_p, sp.first);
                            sent = true;
                            if (prepare_to_be_cached.empty()) prepare_to_be_cached = new_p;
                        }
                    }
                    if (sent) ctrl.set_pbft_my_prepare(hwbs->id);
                }
                return prepare_to_be_cached;
            }
        }

        bool pbft_database::should_prepared() {

            auto const &by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
            auto itr = by_prepare_and_num_index.begin();
            if (itr == by_prepare_and_num_index.end()) return false;

            pbft_state_ptr psp = *itr;

            if (psp->is_prepared && (psp->block_num > ctrl.last_irreversible_block_num())) {
                ctrl.set_pbft_prepared((*itr)->block_id);
                return true;
            }
            return false;
        }

        bool pbft_database::is_valid_prepare(const pbft_prepare &p, const public_key_type &pk) {
            // a prepare msg under lscb (which is no longer in fork_db), can be treated as null, thus true.
            if (p.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            return should_recv_pbft_msg(pk);
        }

        void pbft_database::add_pbft_commit(pbft_commit &c, const public_key_type &pk) {

            auto &by_block_id_index = pbft_state_index.get<by_block_id>();

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
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr &psp) {
                            psp->commits[std::make_pair(c.view, pk)] = c;
                            std::sort(psp->commits.begin(), psp->commits.end(), less<>());
                        });
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
                    for (auto const &com: commits) {
                        if (commit_count.find(com.second.view) == commit_count.end()) commit_count[com.second.view] = 0;
                    }

                    for (auto const &bp: as) {
                        for (auto const &pc: commits) {
                            if (bp.block_signing_key == pc.first.second) commit_count[pc.first.first] += 1;
                        }
                    }

                    for (auto const &e: commit_count) {
                        if (e.second >= threshold) {
                            mark_as_committed(cpsp->block_id);
                        }
                    }
                }
                current = ctrl.fetch_block_state_by_id(current->prev());
            }
        }

        pbft_commit pbft_database::send_and_add_pbft_commit(const pbft_commit &cached_commit, pbft_view_type current_view) {
            auto commit_to_be_cached = pbft_commit();

            if (!cached_commit.empty()) {
                for (auto const &sp : ctrl.my_signature_providers()) {
                    //sign again, update cache, then emit
                    auto retry_c = cached_commit;
                    retry_c.common.timestamp = time_point::now();
                    retry_c.sender_signature = sp.second(retry_c.digest(chain_id));
                    emit(pbft_outgoing_commit, std::make_shared<pbft_commit>(retry_c));
                }
                return commit_to_be_cached;
            } else {
                auto const &by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
                auto itr = by_prepare_and_num_index.begin();
                if (itr == by_prepare_and_num_index.end()) return commit_to_be_cached;

                pbft_state_ptr psp = *itr;
                auto bs = ctrl.fork_db().get_block(psp->block_id);
                if (!bs) return commit_to_be_cached;

                if (psp->is_prepared && (psp->block_num > ctrl.last_irreversible_block_num())) {

                    for (auto const &sp : ctrl.my_signature_providers()) {
                        pbft_commit new_c;
                        new_c.view=current_view;
                        new_c.block_info={psp->block_id};
                        new_c.sender_signature = sp.second(new_c.digest(chain_id));

                        if (is_valid_commit(new_c, sp.first)) {
                            emit(pbft_outgoing_commit, std::make_shared<pbft_commit>(new_c));
                            add_pbft_commit(new_c, sp.first);
                            if (commit_to_be_cached.empty()) commit_to_be_cached = new_c;
                        }
                    }
                }
                return commit_to_be_cached;
            }
        }

        void  pbft_database::mark_as_committed(const block_id_type &bid) {
            auto &by_block_id_index = pbft_state_index.get<by_block_id>();
            auto itr = by_block_id_index.find(bid);
            if (itr == by_block_id_index.end()) return;
            by_block_id_index.modify(itr, [&](const pbft_state_ptr &p) { p->is_committed = true; });
        }

        bool pbft_database::should_committed() {
            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return false;
            pbft_state_ptr psp = *itr;

            return (psp->is_committed && (psp->block_num > ctrl.last_irreversible_block_num()));
        }

        pbft_view_type pbft_database::get_committed_view() {
            pbft_view_type new_view = 0;
            if (!should_committed()) return new_view;

            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            pbft_state_ptr psp = *itr;

            auto blk_state = ctrl.fetch_block_state_by_id((*itr)->block_id);
            if (!blk_state) return new_view;
            auto as = blk_state->active_schedule.producers;

            auto commits = (*itr)->commits;

            auto threshold = as.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> commit_count;
            for (auto const &com: commits) {
                if (commit_count.find(com.second.view) == commit_count.end()) commit_count[com.second.view] = 0;
            }

            for (auto const &bp: as) {
                for (auto const &pc: commits) {
                    if (bp.block_signing_key == pc.first.second) commit_count[pc.first.first] += 1;
                }
            }

            for (auto const &e: commit_count) {
                if (e.second >= threshold && e.first > new_view) {
                    new_view = e.first;
                }
            }
            return new_view;
        }

        bool pbft_database::is_valid_commit(const pbft_commit &c, const public_key_type &pk) {
            if (c.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            return should_recv_pbft_msg(pk);
        }

        void pbft_database::commit_local() {
            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return;

            pbft_state_ptr psp = *itr;

            ctrl.pbft_commit_local(psp->block_id);
        }

        bool pbft_database::pending_pbft_lib() {
            return ctrl.pending_pbft_lib();
        }

        void pbft_database::add_pbft_view_change(pbft_view_change &vc, const public_key_type &pk) {

            auto lscb_bps = lscb_active_producers().producers;

            auto &by_view_index = view_state_index.get<by_view>();
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
                    by_view_index.modify(itr, [&](const pbft_view_change_state_ptr &pvsp) {
                        pvsp->view_changes[pk] = vc;
                    });
                }
            }

            itr = by_view_index.find(vc.target_view);
            if (itr == by_view_index.end()) return;

            auto vsp = *itr;
            auto threshold = lscb_bps.size() * 2 / 3 + 1;
            if (vsp->view_changes.size() >= threshold && !vsp->is_view_changed) {
                auto vc_count = 0;

                for (auto const &bp: lscb_bps) {
                    for (auto const &v: vsp->view_changes) {
                        if (bp.block_signing_key == v.first) vc_count += 1;
                    }
                }
                if (vc_count >= threshold) {
                    by_view_index.modify(itr, [&](const pbft_view_change_state_ptr &pvsp) { pvsp->is_view_changed = true; });
                }
            }
        }

        pbft_view_type pbft_database::should_view_change() {
            pbft_view_type nv = 0;
            auto &by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.begin();
            if (itr == by_view_index.end()) return nv;

            while (itr != by_view_index.end()) {
                auto active_bps = lscb_active_producers().producers;
                auto vc_count = 0;
                auto pvs = (*itr);

                for (auto const &bp: active_bps) {
                    for (auto const &v: pvs->view_changes) {
                        if (bp.block_signing_key == v.first) vc_count += 1;
                    }
                }
                //if contains self or view_change >= f+1, transit to view_change and send view change
                if (vc_count >= active_bps.size() / 3 + 1) {
                    nv = pvs->view;
                    break;
                }
                ++itr;
            }
            return nv;
        }

        pbft_view_change pbft_database::send_and_add_pbft_view_change(
                const pbft_view_change &cached_view_change,
                const pbft_prepared_certificate &ppc,
                const vector<pbft_committed_certificate> &pcc,
                pbft_view_type current_view,
                pbft_view_type target_view) {

            auto view_change_to_be_cached = pbft_view_change();
            if (!cached_view_change.empty()) {
                for (auto const &sp : ctrl.my_signature_providers()) {
                    //sign again, update cache, then emit
                    auto retry_vc = cached_view_change;
                    retry_vc.common.timestamp = time_point::now();
                    retry_vc.sender_signature = sp.second(retry_vc.digest(chain_id));
                    emit(pbft_outgoing_view_change, std::make_shared<pbft_view_change>(retry_vc));
                }
                return view_change_to_be_cached;
            } else {
                for (auto const &my_sp : ctrl.my_signature_providers()) {

                    auto my_lsc = get_stable_checkpoint_by_id(ctrl.last_stable_checkpoint_block_id());

                    pbft_view_change new_vc;
                    new_vc.current_view=current_view;
                    new_vc.target_view=target_view;
                    new_vc.prepared_cert=ppc;
                    new_vc.committed_certs=pcc;
                    new_vc.stable_checkpoint=my_lsc;
                    new_vc.sender_signature = my_sp.second(new_vc.digest(chain_id));
                    if (is_valid_view_change(new_vc, my_sp.first)) {
                        emit(pbft_outgoing_view_change, std::make_shared<pbft_view_change>(new_vc));
                        add_pbft_view_change(new_vc, my_sp.first);
                        if (view_change_to_be_cached.empty()) view_change_to_be_cached = new_vc;
                    }
                }
                return view_change_to_be_cached;
            }
        }

        bool pbft_database::should_new_view(const pbft_view_type target_view) {
            auto &by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(target_view);
            if (itr == by_view_index.end()) return false;
            return (*itr)->is_view_changed;
        }

        pbft_view_type pbft_database::get_proposed_new_view_num() {
            auto &by_count_and_view_index = view_state_index.get<by_count_and_view>();
            auto itr = by_count_and_view_index.begin();
            if (itr == by_count_and_view_index.end() || !(*itr)->is_view_changed) return 0;
            return (*itr)->view;
        }

        bool pbft_database::has_new_primary(const public_key_type &pk) {

            if (pk == public_key_type()) return false;
            auto sps = ctrl.my_signature_providers();
            auto sp_itr = sps.find(pk);
            return sp_itr != sps.end();
        }

        void pbft_database::cleanup_on_new_view() {
            view_state_index.clear();
            ctrl.reset_pbft_my_prepare();
        }

        pbft_new_view pbft_database::send_pbft_new_view(
                const pbft_view_changed_certificate &vcc,
                pbft_view_type current_view) {

            auto primary_key = get_new_view_primary_key(current_view);
            if (!has_new_primary(primary_key) || vcc.empty()) return pbft_new_view();

            //`sp_itr` is not possible to be the end iterator, since it's already been checked in `has_new_primary`.
            auto my_sps = ctrl.my_signature_providers();
            auto sp_itr = my_sps.find(primary_key);

            auto highest_ppc = pbft_prepared_certificate();
            auto highest_pcc = vector<pbft_committed_certificate>{};
            auto highest_sc = pbft_stable_checkpoint();

            for (auto const &vc: vcc.view_changes) {
                if (vc.prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()) {
                    highest_ppc = vc.prepared_cert;
                }

                for (auto const &cc: vc.committed_certs) {
                    auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                            [&](const pbft_committed_certificate &ext) { return ext.block_info.block_id == cc.block_info.block_id; });
                    if (p_itr == highest_pcc.end()) highest_pcc.emplace_back(cc);
                }

                if (vc.stable_checkpoint.block_info.block_num() > highest_sc.block_info.block_num()) {
                    highest_sc = vc.stable_checkpoint;
                }
            }

            pbft_new_view nv;
            nv.new_view=current_view;
            nv.prepared_cert=highest_ppc;
            nv.committed_certs=highest_pcc;
            nv.stable_checkpoint=highest_sc;
            nv.view_changed_cert=vcc;
            nv.sender_signature = sp_itr->second(nv.digest(chain_id));
            try {
                validate_new_view(nv, sp_itr->first);
                emit(pbft_outgoing_new_view, std::make_shared<pbft_new_view>(nv));
                return nv;
            } catch (const fc::exception& ex) {
                elog("bad new view, ${s} ", ("s", ex.to_string()));
                return pbft_new_view();
            }
        }

        pbft_prepared_certificate pbft_database::generate_prepared_certificate() {

            auto const &by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
            auto itr = by_prepare_and_num_index.begin();
            if (itr == by_prepare_and_num_index.end()) return pbft_prepared_certificate();
            pbft_state_ptr psp = *itr;

            auto prepared_block_state = ctrl.fetch_block_state_by_id(psp->block_id);
            if (!prepared_block_state) return pbft_prepared_certificate();

            auto as = prepared_block_state->active_schedule.producers;
            if (psp->is_prepared && psp->block_num > ctrl.last_irreversible_block_num()) {
                auto prepares = psp->prepares;
                auto valid_prepares = vector<pbft_prepare>{};

                flat_map<pbft_view_type, uint32_t> prepare_count;
                flat_map<pbft_view_type, vector<pbft_prepare>> prepare_msg;

                for (auto const &pre: prepares) {
                    if (prepare_count.find(pre.first.first) == prepare_count.end()) prepare_count[pre.first.first] = 0;
                    prepare_msg[pre.first.first].emplace_back(pre.second);
                }

                for (auto const &bp: as) {
                    for (auto const &pp: prepares) {
                        if (bp.block_signing_key == pp.first.second) prepare_count[pp.first.first] += 1;
                    }
                }

                auto bp_threshold = as.size() * 2 / 3 + 1;
                for (auto const &e: prepare_count) {
                    if (e.second >= bp_threshold) {
                        valid_prepares = prepare_msg[e.first];
                    }
                }

                if (valid_prepares.empty()) return pbft_prepared_certificate();

                pbft_prepared_certificate pc;
                pc.block_info={psp->block_id}; pc.prepares=valid_prepares; pc.pre_prepares.emplace(psp->block_id);
                for (auto const &p: valid_prepares) {
                    auto bid = p.block_info.block_id;
                    while (bid != psp->block_id) {
                        pc.pre_prepares.emplace(bid);
                        bid = ctrl.fetch_block_state_by_id(bid)->prev();
                    }
                }
                return pc;
            } else return pbft_prepared_certificate();
        }

        vector<pbft_committed_certificate> pbft_database::generate_committed_certificate() {

            auto pcc = vector<pbft_committed_certificate>{};

            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
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

            auto const &by_id_index = pbft_state_index.get<by_block_id>();

            std::sort(ccb.begin(), ccb.end());
            pcc.reserve(ccb.size());
            for (auto const &committed_block_num: ccb) {
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

                for (auto const &com: commits) {
                    if (commit_count.find(com.first.first) == commit_count.end()) commit_count[com.first.first] = 0;
                    commit_msg[com.first.first].emplace_back(com.second);
                }

                for (auto const &bp: as) {
                    for (auto const &cc: commits) {
                        if (bp.block_signing_key == cc.first.second) commit_count[cc.first.first] += 1;
                    }
                }

                auto bp_threshold = as.size() * 2 / 3 + 1;
                for (auto const &e: commit_count) {
                    if (e.second >= bp_threshold) {
                        valid_commits = commit_msg[e.first];
                    }
                }

                if (valid_commits.empty()) return pcc;

                pbft_committed_certificate cc;
                cc.block_info={cbs->id}; cc.commits=valid_commits;
                pcc.emplace_back(cc);
            }
            return pcc;
        }

        pbft_view_changed_certificate pbft_database::generate_view_changed_certificate(pbft_view_type target_view) {

            auto pvcc = pbft_view_changed_certificate();

            auto &by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(target_view);
            if (itr == by_view_index.end()) return pvcc;

            auto pvs = *itr;

            if (pvs->is_view_changed) {

                pvcc.target_view=pvs->view;
                pvcc.view_changes.reserve(pvs->view_changes.size());
                for(auto & view_change : pvs->view_changes) {
                    pvcc.view_changes.emplace_back( view_change.second );
                }
                return pvcc;
            } else return pvcc;
        }



        bool pbft_database::is_valid_prepared_certificate(const pbft_prepared_certificate &certificate, bool add_to_pbft_db) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lscb (no longer in fork_db) is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;

            auto prepares = certificate.prepares;
            auto prepares_metadata = vector<pbft_message_metadata<pbft_prepare>>{};
            prepares_metadata.reserve(prepares.size());

            for (auto &p : prepares) {
                auto pmm = pbft_message_metadata<pbft_prepare>(p, chain_id);
                prepares_metadata.emplace_back(pmm);
                if (!is_valid_prepare(p, pmm.sender_key)) return false;
                if (add_to_pbft_db) add_pbft_prepare(p, pmm.sender_key);
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> prepare_count;

            for (auto const &pm: prepares_metadata) {
                if (prepare_count.find(pm.msg.view) == prepare_count.end()) prepare_count[pm.msg.view] = 0;
            }

            for (auto const &bp: producer_schedule.producers) {
                for (auto const &pm: prepares_metadata) {
                    if (bp.block_signing_key == pm.sender_key) prepare_count[pm.msg.view] += 1;
                }
            }

            auto should_prepared = false;

            for (auto const &e: prepare_count) {
                if (e.second >= bp_threshold) {
                    should_prepared = true;
                }
            }

            if (!should_prepared) return false;

            //validate prepare
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto non_fork_bp_count = 0;
            vector<block_info_type> prepare_infos;
            prepare_infos.reserve(certificate.prepares.size());
            for (auto const &p : certificate.prepares) {
                //only search in fork db
                if (p.block_info.block_num() <= lscb_num) {
                    ++non_fork_bp_count;
                } else {
                    prepare_infos.emplace_back(p.block_info);
                }
            }
            return is_valid_longest_fork(certificate.block_info, prepare_infos, bp_threshold, non_fork_bp_count);
        }

        bool pbft_database::is_valid_committed_certificate(const pbft_committed_certificate &certificate, bool add_to_pbft_db) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lscb (no longer in fork_db) is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;

            auto commits = certificate.commits;
            auto commits_metadata = vector<pbft_message_metadata<pbft_commit>>{};
            commits_metadata.reserve(commits.size());

            for (auto &c : commits) {
                auto pmm = pbft_message_metadata<pbft_commit>(c, chain_id);
                commits_metadata.emplace_back(pmm);
                if (!is_valid_commit(c, pmm.sender_key)) return false;
                if (add_to_pbft_db) add_pbft_commit(c, pmm.sender_key);
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            flat_map<pbft_view_type, uint32_t> commit_count;

            for (auto const &cm: commits_metadata) {
                if (commit_count.find(cm.msg.view) == commit_count.end()) commit_count[cm.msg.view] = 0;
            }

            for (auto const &bp: producer_schedule.producers) {
                for (auto const &cm: commits_metadata) {
                    if (bp.block_signing_key == cm.sender_key) commit_count[cm.msg.view] += 1;
                }
            }

            auto should_committed = false;

            for (auto const &e: commit_count) {
                if (e.second >= bp_threshold) {
                    should_committed = true;
                }
            }

            if (!should_committed) return false;

            //validate commit
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto non_fork_bp_count = 0;
            vector<block_info_type> commit_infos;
            commit_infos.reserve(certificate.commits.size());
            for (auto const &c : certificate.commits) {
                //only search in fork db
                if (c.block_info.block_num() <= lscb_num) {
                    ++non_fork_bp_count;
                } else {
                    commit_infos.emplace_back(c.block_info);
                }
            }
            return is_valid_longest_fork(certificate.block_info, commit_infos, bp_threshold, non_fork_bp_count);
        }

        bool pbft_database::is_valid_view_change(const pbft_view_change &vc, const public_key_type &pk) {

            return should_recv_pbft_msg(pk);
            // No need to check prepared cert and stable checkpoint, until generate or validate a new view msg
        }


        void pbft_database::validate_new_view(const pbft_new_view &nv, const public_key_type &pk) {

            EOS_ASSERT(pk == get_new_view_primary_key(nv.new_view), pbft_exception,
                       "new view is not signed with expected key");

            EOS_ASSERT(is_valid_prepared_certificate(nv.prepared_cert, true), pbft_exception,
                       "bad prepared certificate: ${pc}", ("pc", nv.prepared_cert));

            EOS_ASSERT(is_valid_stable_checkpoint(nv.stable_checkpoint, true), pbft_exception,
                       "bad stable checkpoint: ${scp}", ("scp", nv.stable_checkpoint));

            auto committed_certs = nv.committed_certs;
            std::sort(committed_certs.begin(), committed_certs.end());
            for (auto const &c: committed_certs) {
                EOS_ASSERT(is_valid_committed_certificate(c, true), pbft_exception,
                           "bad committed certificate: ${cc}", ("cc", c));
            }

            EOS_ASSERT(nv.view_changed_cert.target_view == nv.new_view, pbft_exception, "target view not match");

            vector<public_key_type> lscb_producers;
            lscb_producers.reserve(lscb_active_producers().producers.size());
            for (auto const &bp: lscb_active_producers().producers) {
                lscb_producers.emplace_back(bp.block_signing_key);
            }
            auto schedule_threshold = lscb_producers.size() * 2 / 3 + 1;

            auto view_changes = nv.view_changed_cert.view_changes;
            auto view_changes_metadata = vector<pbft_message_metadata<pbft_view_change>>{};
            view_changes_metadata.reserve(view_changes.size());

            vector<public_key_type> view_change_producers;
            view_change_producers.reserve(view_changes.size());
            for (auto &vc: view_changes) {
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

            for (auto const &vc: nv.view_changed_cert.view_changes) {
                if (vc.prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()
                    && is_valid_prepared_certificate(vc.prepared_cert)) {
                    highest_ppc = vc.prepared_cert;
                }

                for (auto const &cc: vc.committed_certs) {
                    if (is_valid_committed_certificate(cc)) {
                        auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                                             [&](const pbft_committed_certificate &ext) {
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

            EOS_ASSERT(highest_scp.block_info == nv.stable_checkpoint.block_info, pbft_exception,
                       "stable checkpoint does not match, should be ${hscp} but ${scp} given",
                       ("hpcc", highest_scp)("pc", nv.stable_checkpoint));
        }

        bool pbft_database::should_stop_view_change(const pbft_view_change &vc) {
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto vc_lscb = vc.stable_checkpoint.block_info.block_num();
            return vc_lscb > 0 && lscb_num > vc_lscb;
        }

        vector<vector<block_info_type>> pbft_database::fetch_fork_from(vector<block_info_type> &block_infos) {

            vector<vector<block_info_type>> result;
            if (block_infos.empty()) {
                return result;
            }
            if (block_infos.size() == 1) {
                result.emplace_back(initializer_list<block_info_type>{block_infos.front()});
                return result;
            }

            sort(block_infos.begin(), block_infos.end(),
                 [](const block_info_type &a, const block_info_type &b) -> bool { return a.block_num() > b.block_num(); });

            while (!block_infos.empty()) {
                auto fork = fetch_first_fork_from(block_infos);
                if (!fork.empty()) {
                    result.emplace_back(fork);
                }
            }
            return result;
        }

        vector<block_info_type> pbft_database::fetch_first_fork_from(vector<block_info_type> &bi) {
            vector<block_info_type> result;
            if (bi.empty()) {
                return result;
            }
            if (bi.size() == 1) {
                result.emplace_back(bi.front());
                bi.clear();
                return result;
            }
            //bi should be sorted desc
            auto high = bi.front().block_num();
            auto low = bi.back().block_num();

            auto id = bi.front().block_id;
            auto num = bi.front().block_num();
            while (num <= high && num >= low && !bi.empty()) {
                auto bs = ctrl.fetch_block_state_by_id(id);

                for (auto it = bi.begin(); it != bi.end();) {
                    if (it->block_id == id) {
                        if (bs) {
                            //add to result only if b exist
                            result.emplace_back((*it));
                        }
                        it = bi.erase(it);
                    } else {
                        it++;
                    }
                }
                if (bs) {
                    id = bs->prev();
                    num--;
                } else {
                    break;
                }
            }

            return result;
        }

        bool pbft_database::is_valid_longest_fork(const block_info_type &bi, vector<block_info_type> block_infos, unsigned long threshold, unsigned long non_fork_bp_count) {

            auto forks = fetch_fork_from(block_infos);
            vector<block_info_type> longest_fork;
            for (auto const &f : forks) {
                if (f.size() > longest_fork.size()) {
                    longest_fork = f;
                }
            }
            if (longest_fork.size() + non_fork_bp_count < threshold) return false;

            if (longest_fork.empty()) return true;

            auto calculated_block_info = longest_fork.back();

            return bi.block_id == calculated_block_info.block_id;
        }

        pbft_stable_checkpoint pbft_database::fetch_stable_checkpoint_from_blk_extn(const signed_block_ptr &b) {
            try {
                if (b) {
                    auto &extn = b->block_extensions;

                    for (auto it = extn.begin(); it != extn.end();) {
                        if (it->first == static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint)) {
                            auto scp_ds = it->second;
                            fc::datastream<char *> ds(scp_ds.data(), scp_ds.size());

                            pbft_stable_checkpoint scp;
                            fc::raw::unpack(ds, scp);

                            if (is_valid_stable_checkpoint(scp)) {
                                return scp;
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
            }
            return pbft_stable_checkpoint();
        }

        pbft_stable_checkpoint pbft_database::get_stable_checkpoint_by_id(const block_id_type &block_id, bool incl_blk_extn ) {
            auto const &by_block = checkpoint_index.get<by_block_id>();
            auto itr = by_block.find(block_id);
            if (itr == by_block.end()) {
                if (incl_blk_extn) {
                    auto blk = ctrl.fetch_block_by_id(block_id);
                    return fetch_stable_checkpoint_from_blk_extn(blk);
                }
                return pbft_stable_checkpoint();
            }

            auto cpp = *itr;

            if (cpp->is_stable) {
                pbft_stable_checkpoint psc;
                psc.block_info={cpp->block_id};
                psc.checkpoints.reserve(cpp->checkpoints.size());
                for (auto & checkpoint : cpp->checkpoints) {
                    psc.checkpoints.emplace_back(checkpoint.second) ;
                }
                return psc;
            } else return pbft_stable_checkpoint();
        }

        block_info_type pbft_database::cal_pending_stable_checkpoint() const {

            auto pending_scb_num = ctrl.last_stable_checkpoint_block_num();
            auto pending_scb_info = block_info_type{ctrl.last_stable_checkpoint_block_id()};

            auto const &by_blk_num = checkpoint_index.get<by_num>();
            auto itr = by_blk_num.lower_bound(pending_scb_num);
            if (itr == by_blk_num.end()) return pending_scb_info;

            while (itr != by_blk_num.end()) {
                if (auto bs = ctrl.fetch_block_state_by_id((*itr)->block_id)) {
                    auto scb = ctrl.fetch_block_state_by_number(pending_scb_num);

                    auto head_checkpoint_schedule = bs->active_schedule;

                    producer_schedule_type current_schedule;
                    producer_schedule_type new_schedule;

                    if (pending_scb_num == 0) {
                        auto const &ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
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
            auto checkpoint = [&](const block_num_type &in) {
                auto const& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                if (!ctrl.is_pbft_enabled()) return false;
                if (in <= ucb) return false;
                auto watermarks = get_updated_watermarks();
                return in == ucb + 1 // checkpoint on first pbft block;
                       || in % pbft_checkpoint_granularity == 1 // checkpoint on every 100 block;
                       || std::find(watermarks.begin(), watermarks.end(), in) != watermarks.end(); // checkpoint on bp schedule change;
            };

            auto new_pc = vector<pbft_checkpoint>{};

            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end() || !(*itr)->is_committed) return new_pc;

            pbft_state_ptr psp = (*itr);

            flat_map<block_num_type, bool> pending_checkpoint_block_num; // block_height and retry_flag
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            for (auto i = psp->block_num; i > lscb_num && i > 1; --i) {
                if (checkpoint(i)) {
                    auto &by_block = checkpoint_index.get<by_block_id>();

                    if (auto bs = ctrl.fork_db().get_block_in_current_chain_by_num(i)) {
                        auto c_itr = by_block.find(bs->id);
                        if (c_itr == by_block.end()) {
                            pending_checkpoint_block_num[i] = false;
                        } else {
                            auto checkpoints = (*c_itr)->checkpoints;
                            for (auto const &my_sp : ctrl.my_signature_providers()) {
                                if (checkpoints.find(my_sp.first) != checkpoints.end() && !(*c_itr)->is_stable) {
                                    pending_checkpoint_block_num[i] = true; //retry sending at this time.
                                }
                            }
                            if (pending_checkpoint_block_num.find(i) == pending_checkpoint_block_num.end()) {
                                pending_checkpoint_block_num[i] = false;
                            }
                        }
                    }
                }
            }
            auto &by_block = checkpoint_index.get<by_block_id>();

            if (!pending_checkpoint_block_num.empty()) {
                std::sort(pending_checkpoint_block_num.begin(), pending_checkpoint_block_num.end());
                for (auto& bnum_and_retry: pending_checkpoint_block_num) {
                    if (auto bs = ctrl.fork_db().get_block_in_current_chain_by_num(bnum_and_retry.first)) {
                        for (auto const &my_sp : ctrl.my_signature_providers()) {
                            pbft_checkpoint cp;
                            cp.block_info={bs->id};
                            cp.sender_signature = my_sp.second(cp.digest(chain_id));
                            if (!bnum_and_retry.second && is_valid_checkpoint(cp, my_sp.first)) { //first time sending this checkpoint
                                add_pbft_checkpoint(cp, my_sp.first);
                            }
                            new_pc.emplace_back(cp);
                        }
                    }
                }
            } else if (lscb_num > 0) { //retry sending my lscb
                for (auto const &my_sp : ctrl.my_signature_providers()) {
                    pbft_checkpoint cp;
                    cp.block_info={ctrl.last_stable_checkpoint_block_id()};
                    cp.sender_signature = my_sp.second(cp.digest(chain_id));
                    new_pc.emplace_back(cp);
                }
            }
            return new_pc;
        }

        void pbft_database::add_pbft_checkpoint(pbft_checkpoint &cp, const public_key_type &pk) {

            auto cp_block_state = ctrl.fetch_block_state_by_id(cp.block_info.block_id);
            if (!cp_block_state) return;

            auto &by_block = checkpoint_index.get<by_block_id>();
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
                    by_block.modify(itr, [&](const pbft_checkpoint_state_ptr &pcp) {
                        csp->checkpoints[pk] = cp;
                    });
                }
            }

            auto csp = (*itr);
            auto active_bps = cp_block_state->active_schedule.producers;
            auto threshold = active_bps.size() * 2 / 3 + 1;
            if (csp->checkpoints.size() >= threshold && !csp->is_stable) {
                auto cp_count = 0;

                for (auto const &bp: active_bps) {
                    for (auto const &c: csp->checkpoints) {
                        if (bp.block_signing_key == c.first) cp_count += 1;
                    }
                }
                if (cp_count >= threshold) {
                    by_block.modify(itr, [&](const pbft_checkpoint_state_ptr &pcp) { csp->is_stable = true; });
                    auto id = csp->block_id;
                    auto blk = ctrl.fetch_block_by_id(id);

                    if (blk && (blk->block_extensions.empty() || blk->block_extensions.back().first != static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint))) {
                        auto scp = get_stable_checkpoint_by_id(id);
                        auto scp_size = fc::raw::pack_size(scp);

                        auto buffer = std::make_shared<vector<char>>(scp_size);
                        fc::datastream<char*> ds( buffer->data(), scp_size);
                        fc::raw::pack( ds, scp );

                        blk->block_extensions.emplace_back();
                        auto &extension = blk->block_extensions.back();
                        extension.first = static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint );
                        extension.second.resize(scp_size);
                        std::copy(buffer->begin(),buffer->end(), extension.second.data());
                    }
                }
            }
        }

        void pbft_database::send_pbft_checkpoint() {
            auto cps_to_send = generate_and_add_pbft_checkpoint();
            for (auto const &cp: cps_to_send) {
                emit(pbft_outgoing_checkpoint, std::make_shared<pbft_checkpoint>(cp));
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
                auto &by_block_id_index = pbft_state_index.get<by_block_id>();
                auto pitr = by_block_id_index.find(pending_id);
                if (pitr != by_block_id_index.end()) {
                    prune(*pitr);
                }
            }
            auto &bni = checkpoint_index.get<by_num>();
            auto oldest = bni.begin();
            if ( oldest != bni.end()
                 && (*oldest)->is_stable
                 && (*oldest)->block_num < lscb_num - oldest_stable_checkpoint ) {
                prune(*oldest);
            }
        }

        bool pbft_database::is_valid_checkpoint(const pbft_checkpoint &cp, const public_key_type &pk) {

            if (cp.block_info.block_num() > ctrl.head_block_num() || cp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return false;

            if (auto bs = ctrl.fetch_block_state_by_id(cp.block_info.block_id)) {
                auto active_bps = bs->active_schedule.producers;
                for (auto const &bp: active_bps) {
                    if (bp.block_signing_key == pk) return true;
                }
            }
            return false;
        }

        bool pbft_database::is_valid_stable_checkpoint(const pbft_stable_checkpoint &scp, bool add_to_pbft_db) {
            if (scp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num())
                // the stable checkpoint is way behind lib, no way getting the block state,
                // it will not be applied nor saved, thus considered safe.
                return true;

            auto checkpoints = scp.checkpoints;
            auto checkpoints_metadata = vector<pbft_message_metadata<pbft_checkpoint>>{};
            checkpoints_metadata.reserve(checkpoints.size());

            for (auto &cp : checkpoints) {
                auto pmm = pbft_message_metadata<pbft_checkpoint>(cp, chain_id);
                checkpoints_metadata.emplace_back(pmm);
                if (cp.block_info != scp.block_info || !is_valid_checkpoint(cp, pmm.sender_key)) return false;
                if (add_to_pbft_db) add_pbft_checkpoint(cp, pmm.sender_key);
            }

            if (add_to_pbft_db) checkpoint_local();


            if (auto bs = ctrl.fetch_block_state_by_number(scp.block_info.block_num())) {
                auto as = bs->active_schedule;
                auto cp_count = 0;
                for (auto const &bp: as.producers) {
                    for (auto const &cpm: checkpoints_metadata) {
                        if (bp.block_signing_key == cpm.sender_key) cp_count += 1;
                    }
                }
                return cp_count >= as.producers.size() * 2 / 3 + 1;
            }
            return false;

        }

        bool pbft_database::should_send_pbft_msg() {

            auto schedules = get_updated_fork_schedules();
            for (auto const &bp: schedules) {
                for (auto const &sp: ctrl.my_signature_providers()) {
                    if (bp.first == sp.first) return true;
                }
            }
            return false;
        }

        bool pbft_database::should_recv_pbft_msg(const public_key_type &pub_key) {

            auto schedules = get_updated_fork_schedules();
            for (auto const &bp: schedules) {
                if (bp.first == pub_key) return true;
            }
            return false;
        }

        public_key_type pbft_database::get_new_view_primary_key(const pbft_view_type target_view) {

            auto active_bps = lscb_active_producers().producers;
            if (active_bps.empty()) return public_key_type();

            return active_bps[target_view % active_bps.size()].block_signing_key;
        }

        producer_schedule_type pbft_database::lscb_active_producers() const {
            auto num = ctrl.last_stable_checkpoint_block_num();

            if (num == 0) {
                auto const &ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                if (ucb == 0) return ctrl.initial_schedule();
                num = ucb;
            }

            if (auto bs = ctrl.fetch_block_state_by_number(num)) {
                if (bs->pending_schedule.producers.empty()) return bs->active_schedule;
                return bs->pending_schedule;
            }
            return ctrl.initial_schedule();
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

            auto vector_minus = [&](vector<block_num_type> &v1, vector<block_num_type> &v2)
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
                        for (auto &bp: as) {
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
            for (auto &bp: lscb_bps) {
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

        bool pbft_database::is_less_than_high_watermark(const block_num_type &bnum) {
            auto current_watermark = get_current_pbft_watermark();
            return current_watermark == 0 || bnum <= current_watermark;
        }

        pbft_state_ptr pbft_database::get_pbft_state_by_id(const block_id_type& id) const {

            auto &by_block_id_index = pbft_state_index.get<by_block_id>();
            auto itr = by_block_id_index.find(id);

            if (itr != by_block_id_index.end()) return (*itr);

            return pbft_state_ptr();
        }

        vector<pbft_checkpoint_state> pbft_database::get_checkpoints_by_num(const block_num_type& num) const {
            auto results = vector<pbft_checkpoint_state>{};
            auto &by_num_index = checkpoint_index.get<by_num>();

            auto pitr  = by_num_index.lower_bound( num );
            while(pitr != by_num_index.end() && (*pitr)->block_num == num ) {
                results.emplace_back(*(*pitr));
                ++pitr;
            }

            return results;
        }

        pbft_view_change_state_ptr pbft_database::get_view_changes_by_target_view(const pbft_view_type& tv) const {
            auto &by_view_index = view_state_index.get<by_view>();
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

        void pbft_database::prune(const pbft_state_ptr &h) {
            auto num = h->block_num;

            auto &by_bn = pbft_state_index.get<by_num>();
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

        void pbft_database::prune(const pbft_checkpoint_state_ptr &h) {
            auto num = h->block_num;

            auto &by_bn = checkpoint_index.get<by_num>();
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

        template<typename Signal, typename Arg>
        void pbft_database::emit(const Signal &s, Arg &&a) {
            try {
                s(std::forward<Arg>(a));
            } catch (boost::interprocess::bad_alloc &e) {
                wlog("bad alloc");
                throw e;
            } catch (controller_emit_signal_exception &e) {
                wlog("${details}", ("details", e.to_detail_string()));
                throw e;
            } catch (fc::exception &e) {
                wlog("${details}", ("details", e.to_detail_string()));
            } catch (...) {
                wlog("signal handler threw exception");
            }
        }
    }
}