#include <eosio/chain/pbft_database.hpp>
#include <fc/io/fstream.hpp>
#include <fstream>
#include <eosio/chain/global_property_object.hpp>

namespace eosio {
    namespace chain {

        pbft_database::pbft_database(controller &ctrl) :
                ctrl(ctrl) {
            checkpoint_index = pbft_checkpoint_state_multi_index_type{};
            view_state_index = pbft_view_state_multi_index_type{};
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
                pbft_state_index = pbft_state_multi_index_type{};
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
                checkpoint_index = pbft_checkpoint_state_multi_index_type{};
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

        void pbft_database::add_pbft_prepare(pbft_prepare &p) {

            if (!is_valid_prepare(p)) return;

            auto &by_block_id_index = pbft_state_index.get<by_block_id>();

            auto current = ctrl.fetch_block_state_by_id(p.block_info.block_id);

            while ((current) && (current->block_num > ctrl.last_irreversible_block_num())) {
                auto curr_itr = by_block_id_index.find(current->id);

                if (curr_itr == by_block_id_index.end()) {
                    try {
                        auto curr_ps = pbft_state{current->id, current->block_num, {p}};
                        auto curr_psp = make_shared<pbft_state>(curr_ps);
                        pbft_state_index.insert(curr_psp);
                    } catch (...) {
                        elog( "prepare insert failure: ${p}", ("p", p));
                    }
                } else {
                    auto prepares = (*curr_itr)->prepares;
                    auto p_itr = find_if(prepares.begin(), prepares.end(),
                                         [&](const pbft_prepare &prep) {
                                             return prep.common.sender == p.common.sender
                                             && prep.view == p.view;
                                         });
                    if (p_itr == prepares.end()) {
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr &psp) {
                            psp->prepares.emplace_back(p);
                            std::sort(psp->prepares.begin(), psp->prepares.end(), less<>());
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
                        if (prepare_count.find(pre.view) == prepare_count.end()) prepare_count[pre.view] = 0;
                    }

                    for (auto const &sp: as) {
                        for (auto const &pp: prepares) {
                            if (sp.block_signing_key == pp.common.sender) prepare_count[pp.view] += 1;
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
                auto ps = pbft_state{bid, bnum, .is_prepared = true};
                auto psp = make_shared<pbft_state>(ps);
                pbft_state_index.insert(psp);
                return;
            }
            by_block_id_index.modify(itr, [&](const pbft_state_ptr &p) { p->is_prepared = true; });
        }

        vector<pbft_prepare> pbft_database::send_and_add_pbft_prepare(const vector<pbft_prepare> &pv, pbft_view_type current_view) {

            auto head_block_num = ctrl.head_block_num();
            if (head_block_num <= 1) return vector<pbft_prepare>{};
            auto my_prepare = ctrl.get_pbft_my_prepare();

            auto reserve_prepare = [&](const block_id_type &in) {
                if (in == block_id_type() || !ctrl.fetch_block_state_by_id(in)) return false;
                auto lib = ctrl.last_irreversible_block_id();
                if (lib == block_id_type()) return true;
                auto forks = ctrl.fork_db().fetch_branch_from(in, lib);
                return !forks.first.empty() && forks.second.empty();
            };

            vector<pbft_prepare> new_pv;
            new_pv.reserve(ctrl.my_signature_providers().size());
            if (!pv.empty()) {
                for (auto p : pv) {
                    //change uuid, sign again, update cache, then emit
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    p.common.uuid = uuid;
                    p.common.timestamp = time_point::now();
                    p.sender_signature = ctrl.my_signature_providers()[p.common.sender](p.digest());
                    emit(pbft_outgoing_prepare, p);
                }
                return vector<pbft_prepare>{};
            } else if (reserve_prepare(my_prepare)) {
                for (auto const &sp : ctrl.my_signature_providers()) {
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    pbft_prepare p;
                    p.common.uuid=uuid; p.view=current_view; p.block_info={my_prepare}; p.common.sender=sp.first; p.common.chain_id=chain_id;
                    p.sender_signature = sp.second(p.digest());
                    emit(pbft_outgoing_prepare, p);
                    new_pv.emplace_back(p);
                }
                return new_pv;
            } else {

                auto current_watermark = get_current_pbft_watermark();
                auto lib = ctrl.last_irreversible_block_num();

                uint32_t high_watermark_block_num = head_block_num;

                if ( current_watermark > 0 ) {
                    high_watermark_block_num = std::min(head_block_num, current_watermark);
                }

                if (high_watermark_block_num <= lib) return vector<pbft_prepare>{};

                if (auto hwbs = ctrl.fork_db().get_block_in_current_chain_by_num(high_watermark_block_num)) {

                    for (auto const &sp : ctrl.my_signature_providers()) {
                        auto uuid = boost::uuids::to_string(uuid_generator());
                        pbft_prepare p;
                        p.common.uuid=uuid; p.view=current_view; p.block_info={hwbs->id}; p.common.sender=sp.first; p.common.chain_id=chain_id;
                        p.sender_signature = sp.second(p.digest());
                        add_pbft_prepare(p);
                        emit(pbft_outgoing_prepare, p);
                        new_pv.emplace_back(p);
                    }
                    ctrl.set_pbft_my_prepare(hwbs->id);
                }
                return new_pv;
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

        bool pbft_database::is_valid_prepare(const pbft_prepare &p) {
            if (!is_valid_pbft_message(p.common)) return false;
            // a prepare msg under lscb (which is no longer in fork_db), can be treated as null, thus true.
            if (p.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            if (!p.is_signature_valid()) return false;
            return should_recv_pbft_msg(p.common.sender);
        }

        void pbft_database::add_pbft_commit(pbft_commit &c) {

            if (!is_valid_commit(c)) return;
            auto &by_block_id_index = pbft_state_index.get<by_block_id>();

            auto current = ctrl.fetch_block_state_by_id(c.block_info.block_id);

            while ((current) && (current->block_num > ctrl.last_irreversible_block_num())) {

                auto curr_itr = by_block_id_index.find(current->id);

                if (curr_itr == by_block_id_index.end()) {
                    try {
                        auto curr_ps = pbft_state{current->id, current->block_num, .commits={c}};
                        auto curr_psp = make_shared<pbft_state>(curr_ps);
                        pbft_state_index.insert(curr_psp);
                    } catch (...) {
                        elog("commit insertion failure: ${c}", ("c", c));
                    }
                } else {
                    auto commits = (*curr_itr)->commits;
                    auto p_itr = find_if(commits.begin(), commits.end(),
                                         [&](const pbft_commit &comm) {
                                             return comm.common.sender == c.common.sender
                                             && comm.view == c.view;
                                         });
                    if (p_itr == commits.end()) {
                        by_block_id_index.modify(curr_itr, [&](const pbft_state_ptr &psp) {
                            psp->commits.emplace_back(c);
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
                        if (commit_count.find(com.view) == commit_count.end()) commit_count[com.view] = 0;
                    }


                    for (auto const &sp: as) {
                        for (auto const &pc: commits) {
                            if (sp.block_signing_key == pc.common.sender) commit_count[pc.view] += 1;
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

        vector<pbft_commit> pbft_database::send_and_add_pbft_commit(const vector<pbft_commit> &cv, pbft_view_type current_view) {
            if (!cv.empty()) {
                for (auto c : cv) {
                    //change uuid, sign again, update cache, then emit
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    c.common.uuid = uuid;
                    c.common.timestamp = time_point::now();
                    c.sender_signature = ctrl.my_signature_providers()[c.common.sender](c.digest());
                    emit(pbft_outgoing_commit, c);
                }
                return vector<pbft_commit>{};
            } else {
                auto const &by_prepare_and_num_index = pbft_state_index.get<by_prepare_and_num>();
                auto itr = by_prepare_and_num_index.begin();
                if (itr == by_prepare_and_num_index.end()) return vector<pbft_commit>{};

                pbft_state_ptr psp = *itr;
                auto bs = ctrl.fork_db().get_block(psp->block_id);
                if (!bs) return vector<pbft_commit>{};

                vector<pbft_commit> new_cv;
                new_cv.reserve(ctrl.my_signature_providers().size());
                if (psp->is_prepared && (psp->block_num > ctrl.last_irreversible_block_num())) {

                    for (auto const &sp : ctrl.my_signature_providers()) {
                        auto uuid = boost::uuids::to_string(uuid_generator());
                        pbft_commit c;
                        c.common.uuid=uuid; c.view=current_view; c.block_info={psp->block_id}; c.common.sender=sp.first; c.common.chain_id=chain_id;
                        c.sender_signature = sp.second(c.digest());
                        add_pbft_commit(c);
                        emit(pbft_outgoing_commit, c);
                        new_cv.emplace_back(c);
                    }
                }
                return new_cv;
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

            flat_map<pbft_view_type, uint32_t> commit_count;
            for (auto const &com: commits) {
                if (commit_count.find(com.view) == commit_count.end()) {
                    commit_count[com.view] = 1;
                } else {
                    commit_count[com.view] += 1;
                }
            }

            for (auto const &e: commit_count) {
                if (e.second >= as.size() * 2 / 3 + 1 && e.first > new_view) {
                    new_view = e.first;
                }
            }
            return new_view;
        }

        bool pbft_database::is_valid_commit(const pbft_commit &c) {
            if (!is_valid_pbft_message(c.common)) return false;
            if (c.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;
            if (!c.is_signature_valid()) return false;
            return should_recv_pbft_msg(c.common.sender);
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

        void pbft_database::add_pbft_view_change(pbft_view_change &vc) {
            if (!is_valid_view_change(vc)) return;
            auto active_bps = lscb_active_producers().producers;

            auto &by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(vc.target_view);
            if (itr == by_view_index.end()) {
                auto vs = pbft_view_change_state{vc.target_view, .view_changes={vc}};
                auto vsp = make_shared<pbft_view_change_state>(vs);
                view_state_index.insert(vsp);
            } else {
                auto pvs = (*itr);
                auto view_changes = pvs->view_changes;
                auto p_itr = find_if(view_changes.begin(), view_changes.end(),
                                     [&](const pbft_view_change &existed) {
                                         return existed.common.sender == vc.common.sender;
                                     });
                if (p_itr == view_changes.end()) {
                    by_view_index.modify(itr, [&](const pbft_view_change_state_ptr &pvsp) {
                        pvsp->view_changes.emplace_back(vc);
                    });
                }
            }

            itr = by_view_index.find(vc.target_view);
            if (itr == by_view_index.end()) return;

            auto vsp = *itr;
            auto threshold = active_bps.size() * 2 / 3 + 1;
            if (vsp->view_changes.size() >= threshold && !vsp->is_view_changed) {
                auto vc_count = 0;

                for (auto const &sp: active_bps) {
                    for (auto const &v: (*itr)->view_changes) {
                        if (sp.block_signing_key == v.common.sender) vc_count += 1;
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
                    for (auto const &pp: pvs->view_changes) {
                        if (bp.block_signing_key == pp.common.sender) vc_count += 1;
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

        vector<pbft_view_change> pbft_database::send_and_add_pbft_view_change(
                const vector<pbft_view_change> &vcv,
                const pbft_prepared_certificate &ppc,
                const vector<pbft_committed_certificate> &pcc,
                pbft_view_type current_view,
                pbft_view_type new_view) {
            if (!vcv.empty()) {
                for (auto vc : vcv) {
                    //change uuid, sign again, update cache, then emit
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    vc.common.uuid = uuid;
                    vc.common.timestamp = time_point::now();
                    vc.sender_signature = ctrl.my_signature_providers()[vc.common.sender](vc.digest());
                    emit(pbft_outgoing_view_change, vc);
                }
                return vector<pbft_view_change>{};
            } else {
                vector<pbft_view_change> new_vcv;
                new_vcv.reserve(ctrl.my_signature_providers().size());
                for (auto const &my_sp : ctrl.my_signature_providers()) {

                    auto my_lsc = get_stable_checkpoint_by_id(ctrl.last_stable_checkpoint_block_id());
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    pbft_view_change vc;
                    vc.common.uuid=uuid; vc.current_view=current_view; vc.target_view=new_view; vc.prepared_cert=ppc; vc.committed_cert=pcc; vc.stable_checkpoint=my_lsc; vc.common.sender=my_sp.first; vc.common.chain_id=chain_id;
                    vc.sender_signature = my_sp.second(vc.digest());
                    emit(pbft_outgoing_view_change, vc);
                    add_pbft_view_change(vc);
                    new_vcv.emplace_back(vc);
                }
                return new_vcv;
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

        bool pbft_database::is_new_primary(const pbft_view_type target_view) {

            auto primary_key = get_new_view_primary_key(target_view);
            if (primary_key == public_key_type()) return false;
            auto sps = ctrl.my_signature_providers();
            auto sp_itr = sps.find(primary_key);
            return sp_itr != sps.end();
        }

        void pbft_database::prune_pbft_index() {
            view_state_index.clear();
            ctrl.reset_pbft_my_prepare();
        }

        pbft_new_view pbft_database::send_pbft_new_view(
                const pbft_view_changed_certificate &vcc,
                pbft_view_type current_view) {

            auto primary_key = get_new_view_primary_key(current_view);
            if (!is_new_primary(current_view) || vcc.empty()) return pbft_new_view();

            //`sp_itr` is not possible to be the end iterator, since it's already been checked in `is_new_primary`.
            auto my_sps = ctrl.my_signature_providers();
            auto sp_itr = my_sps.find(primary_key);

            auto highest_ppc = pbft_prepared_certificate();
            auto highest_pcc = vector<pbft_committed_certificate>{};
            auto highest_sc = pbft_stable_checkpoint();

            for (auto const &vc: vcc.view_changes) {
                if (vc.prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()
                    && is_valid_prepared_certificate(vc.prepared_cert)) {
                    highest_ppc = vc.prepared_cert;
                }

                for (auto const &cc: vc.committed_cert) {
                    if (is_valid_committed_certificate(cc)) {
                        auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                                [&](const pbft_committed_certificate &ext) { return ext.block_info.block_id == cc.block_info.block_id; });
                        if (p_itr == highest_pcc.end()) highest_pcc.emplace_back(cc);
                    }
                }

                if (vc.stable_checkpoint.block_info.block_num() > highest_sc.block_info.block_num() &&
                    is_valid_stable_checkpoint(vc.stable_checkpoint)) {
                    highest_sc = vc.stable_checkpoint;
                }
            }

            auto uuid = boost::uuids::to_string(uuid_generator());

            pbft_new_view nv;
            nv.common.uuid=uuid; nv.new_view=current_view; nv.prepared_cert=highest_ppc; nv.committed_cert=highest_pcc; nv.stable_checkpoint=highest_sc, nv.view_changed_cert=vcc, nv.common.sender=sp_itr->first; nv.common.chain_id=chain_id;

            nv.sender_signature = sp_itr->second(nv.digest());
            emit(pbft_outgoing_new_view, nv);
            return nv;
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
                    if (prepare_count.find(pre.view) == prepare_count.end()) prepare_count[pre.view] = 0;
                    prepare_msg[pre.view].emplace_back(pre);
                }

                for (auto const &sp: as) {
                    for (auto const &pp: prepares) {
                        if (sp.block_signing_key == pp.common.sender) prepare_count[pp.view] += 1;
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

            auto const &by_commit_and_num_index = pbft_state_index.get<by_commit_and_num>();
            auto itr = by_commit_and_num_index.begin();
            if (itr == by_commit_and_num_index.end()) return vector<pbft_committed_certificate>{};

            pbft_state_ptr psp = *itr;

            if (!psp->is_committed) return vector<pbft_committed_certificate>{};

            auto highest_committed_block_num = psp->block_num;

            vector<block_num_type> ccb;

            //adding my highest committed cert.
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            if ( highest_committed_block_num > lscb_num ) {
                ccb.emplace_back(highest_committed_block_num);
            }

            auto watermarks = get_updated_watermarks();
            for (auto& watermark : watermarks) {
                //adding committed cert on every water mark.
                if (watermark < highest_committed_block_num && watermark > lscb_num) {
                    ccb.emplace_back(watermark);
                }
            }

            auto const &by_id_index = pbft_state_index.get<by_block_id>();

            auto pcc = vector<pbft_committed_certificate>{};
            pcc.reserve(ccb.size());
            for (auto const &committed_block_num: ccb) {
                auto cbs = ctrl.fetch_block_state_by_number(committed_block_num);
                if (!cbs) return vector<pbft_committed_certificate>{};

                auto it = by_id_index.find(cbs->id);
                if (it == by_id_index.end() || !(*it)->is_committed) {
                    return vector<pbft_committed_certificate>{};
                }

                auto as = cbs->active_schedule.producers;

                auto commits = (*it)->commits;
                auto valid_commits = vector<pbft_commit>{};

                flat_map<pbft_view_type, uint32_t> commit_count;
                flat_map<pbft_view_type, vector<pbft_commit>> commit_msg;

                for (auto const &com: commits) {
                    if (commit_count.find(com.view) == commit_count.end()) commit_count[com.view] = 0;
                    commit_msg[com.view].emplace_back(com);
                }

                for (auto const &sp: as) {
                    for (auto const &cc: commits) {
                        if (sp.block_signing_key == cc.common.sender) commit_count[cc.view] += 1;
                    }
                }

                auto bp_threshold = as.size() * 2 / 3 + 1;
                for (auto const &e: commit_count) {
                    if (e.second >= bp_threshold) {
                        valid_commits = commit_msg[e.first];
                    }
                }

                if (valid_commits.empty()) return vector<pbft_committed_certificate>{};

                pbft_committed_certificate cc;
                cc.block_info={cbs->id}; cc.commits=valid_commits;
                pcc.emplace_back(cc);
            }
            return pcc;
        }

        pbft_view_changed_certificate pbft_database::generate_view_changed_certificate(pbft_view_type target_view) {

            auto &by_view_index = view_state_index.get<by_view>();
            auto itr = by_view_index.find(target_view);
            if (itr == by_view_index.end()) return pbft_view_changed_certificate();

            auto pvs = *itr;

            if (pvs->is_view_changed) {
                auto pvcc = pbft_view_changed_certificate();
                pvcc.target_view=pvs->view; pvcc.view_changes=pvs->view_changes;
                return pvcc;
            } else return pbft_view_changed_certificate();
        }



        bool pbft_database::is_valid_prepared_certificate(const pbft_prepared_certificate &certificate) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lscb (no longer in fork_db) is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;

            auto valid = true;
            for (auto const &p : certificate.prepares) {
                valid = valid && is_valid_prepare(p);
                if (!valid) return false;
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            auto prepares = certificate.prepares;
            flat_map<pbft_view_type, uint32_t> prepare_count;

            for (auto const &pre: prepares) {
                if (prepare_count.find(pre.view) == prepare_count.end()) prepare_count[pre.view] = 0;
            }

            for (auto const &sp: producer_schedule.producers) {
                for (auto const &pp: prepares) {
                    if (sp.block_signing_key == pp.common.sender) prepare_count[pp.view] += 1;
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
            auto lscb = ctrl.last_stable_checkpoint_block_num();
            auto non_fork_bp_count = 0;
            vector<block_info_type> prepare_infos;
            prepare_infos.reserve(certificate.prepares.size());
            for (auto const &p : certificate.prepares) {
                //only search in fork db
                if (p.block_info.block_num() <= lscb) {
                    ++non_fork_bp_count;
                } else {
                    prepare_infos.emplace_back(p.block_info);
                }
            }
            return is_valid_longest_fork(certificate.block_info, prepare_infos, bp_threshold, non_fork_bp_count);
        }

        bool pbft_database::is_valid_committed_certificate(const pbft_committed_certificate &certificate) {
            // an empty certificate is valid since it acts as a null digest in pbft.
            if (certificate.empty()) return true;
            // a certificate under lscb (no longer in fork_db) is also treated as null.
            if (certificate.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()) return true;

            auto valid = true;
            for (auto const &c : certificate.commits) {
                valid = valid && is_valid_commit(c);
                if (!valid) return false;
            }

            auto cert_id = certificate.block_info.block_id;
            auto cert_bs = ctrl.fetch_block_state_by_id(cert_id);
            auto producer_schedule = lscb_active_producers();
            if (certificate.block_info.block_num() > 0 && cert_bs) {
                producer_schedule = cert_bs->active_schedule;
            }
            auto bp_threshold = producer_schedule.producers.size() * 2 / 3 + 1;

            auto commits = certificate.commits;
            flat_map<pbft_view_type, uint32_t> commit_count;

            for (auto const &pre: commits) {
                if (commit_count.find(pre.view) == commit_count.end()) commit_count[pre.view] = 0;
            }

            for (auto const &sp: producer_schedule.producers) {
                for (auto const &pp: commits) {
                    if (sp.block_signing_key == pp.common.sender) commit_count[pp.view] += 1;
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
            auto lscb = ctrl.last_stable_checkpoint_block_num();
            auto non_fork_bp_count = 0;
            vector<block_info_type> commit_infos;
            commit_infos.reserve(certificate.commits.size());
            for (auto const &c : certificate.commits) {
                //only search in fork db
                if (c.block_info.block_num() <= lscb) {
                    ++non_fork_bp_count;
                } else {
                    commit_infos.emplace_back(c.block_info);
                }
            }
            return is_valid_longest_fork(certificate.block_info, commit_infos, bp_threshold, non_fork_bp_count);
        }

        bool pbft_database::is_valid_view_change(const pbft_view_change &vc) {
            if (vc.common.chain_id != chain_id) return false;

            return vc.is_signature_valid()
                   && should_recv_pbft_msg(vc.common.sender);
            // No need to check prepared cert and stable checkpoint, until generate or validate a new view msg
        }


        bool pbft_database::is_valid_new_view(const pbft_new_view &nv) {
            //all signatures should be valid

            EOS_ASSERT(nv.common.chain_id == chain_id, pbft_exception, "wrong chain.");

            EOS_ASSERT(nv.is_signature_valid(), pbft_exception, "bad new view signature");

            EOS_ASSERT(nv.common.sender == get_new_view_primary_key(nv.new_view), pbft_exception, "new view is not signed with expected key");

            EOS_ASSERT(is_valid_prepared_certificate(nv.prepared_cert), pbft_exception,
                    "bad prepared certificate: ${pc}", ("pc", nv.prepared_cert));

            EOS_ASSERT(is_valid_stable_checkpoint(nv.stable_checkpoint), pbft_exception,
                       "bad stable checkpoint: ${scp}", ("scp", nv.stable_checkpoint));

            for (auto const &c: nv.committed_cert) {
                EOS_ASSERT(is_valid_committed_certificate(c), pbft_exception, "bad committed certificate: ${cc}", ("cc", c));
            }

            EOS_ASSERT(nv.is_signature_valid(), pbft_exception, "bad new view signature");

            EOS_ASSERT(nv.view_changed_cert.target_view == nv.new_view, pbft_exception, "target view not match");

            vector<public_key_type> lscb_producers;
            lscb_producers.reserve(lscb_active_producers().producers.size());
            for (auto const& pk: lscb_active_producers().producers) {
                lscb_producers.emplace_back(pk.block_signing_key);
            }
            auto schedule_threshold = lscb_producers.size() * 2 / 3 + 1;

            vector<public_key_type> view_change_producers;
            view_change_producers.reserve(nv.view_changed_cert.view_changes.size());
            for (auto vc: nv.view_changed_cert.view_changes) {
                if (is_valid_view_change(vc)) {
                    add_pbft_view_change(vc);
                    view_change_producers.emplace_back(vc.common.sender);
                }
            }

            vector<public_key_type> intersection;

            std::sort(lscb_producers.begin(),lscb_producers.end());
            std::sort(view_change_producers.begin(),view_change_producers.end());
            std::set_intersection(lscb_producers.begin(),lscb_producers.end(),
                                  view_change_producers.begin(),view_change_producers.end(),
                                  back_inserter(intersection));

            EOS_ASSERT(intersection.size() >= schedule_threshold, pbft_exception, "view changes count not enough");

            EOS_ASSERT(should_new_view(nv.new_view), pbft_exception, "should not enter new view: ${nv}", ("nv", nv.new_view));

            auto highest_ppc = pbft_prepared_certificate();
            auto highest_pcc = vector<pbft_committed_certificate>{};
            auto highest_scp = pbft_stable_checkpoint();

            for (auto const &vc: nv.view_changed_cert.view_changes) {
                if (vc.prepared_cert.block_info.block_num() > highest_ppc.block_info.block_num()
                    && is_valid_prepared_certificate(vc.prepared_cert)) {
                    highest_ppc = vc.prepared_cert;
                }

                for (auto const &cc: vc.committed_cert) {
                    if (is_valid_committed_certificate(cc)) {
                        auto p_itr = find_if(highest_pcc.begin(), highest_pcc.end(),
                                [&](const pbft_committed_certificate &ext) { return ext.block_info.block_id == cc.block_info.block_id; });
                        if (p_itr == highest_pcc.end()) highest_pcc.emplace_back(cc);
                    }
                }

                if (vc.stable_checkpoint.block_info.block_num() > highest_scp.block_info.block_num()
                    && is_valid_stable_checkpoint(vc.stable_checkpoint)) {
                    highest_scp = vc.stable_checkpoint;
                }
            }

            EOS_ASSERT(highest_ppc.block_info == nv.prepared_cert.block_info, pbft_exception,
                    "prepared certificate does not match, should be ${hppc} but ${pc} given",
                    ("hppc",highest_ppc)("pc", nv.prepared_cert));

            std::sort(highest_pcc.begin(), highest_pcc.end());
            auto committed_certs = nv.committed_cert;
            std::sort(committed_certs.begin(), committed_certs.end());
            EOS_ASSERT(highest_pcc.size() == committed_certs.size(), pbft_exception, "wrong committed certificates size");
            for (auto i = 0; i < committed_certs.size(); ++i) {
                EOS_ASSERT(highest_pcc[i].block_info == committed_certs[i].block_info, pbft_exception,
                        "committed certificate does not match, should be ${hpcc} but ${cc} given",
                        ("hpcc",highest_pcc[i])("cc", committed_certs[i]));
            }

            EOS_ASSERT(highest_scp.block_info == nv.stable_checkpoint.block_info, pbft_exception,
                       "stable checkpoint does not match, should be ${hscp} but ${scp} given",
                       ("hpcc",highest_scp)("pc", nv.stable_checkpoint));

            return true;
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
                    auto &ext = b->block_extensions;

                    for (auto it = ext.begin(); it != ext.end();) {
                        if (it->first == static_cast<uint16_t>(block_extension_type::pbft_stable_checkpoint)) {
                            auto scp_v = it->second;
                            fc::datastream<char *> ds_decode(scp_v.data(), scp_v.size());

                            pbft_stable_checkpoint scp_decode;
                            fc::raw::unpack(ds_decode, scp_decode);

                            if (is_valid_stable_checkpoint(scp_decode)) {
                                return scp_decode;
                            } else {
                                it = ext.erase(it);
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

        pbft_stable_checkpoint pbft_database::get_stable_checkpoint_by_id(const block_id_type &block_id) {
            auto const &by_block = checkpoint_index.get<by_block_id>();
            auto itr = by_block.find(block_id);
            if (itr == by_block.end()) {
                auto blk = ctrl.fetch_block_by_id(block_id);
                return fetch_stable_checkpoint_from_blk_extn(blk);
            }

            auto cpp = *itr;

            if (cpp->is_stable) {
                if (ctrl.my_signature_providers().empty()) return pbft_stable_checkpoint();
                pbft_stable_checkpoint psc;
                psc.block_info={cpp->block_id}; psc.checkpoints=cpp->checkpoints;
                return psc;
            } else return pbft_stable_checkpoint();
        }

        block_info_type pbft_database::cal_pending_stable_checkpoint() const {

            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto lscb_id = ctrl.last_stable_checkpoint_block_id();
            auto lscb_info = block_info_type{lscb_id};

            auto const &by_blk_num = checkpoint_index.get<by_num>();
            auto itr = by_blk_num.lower_bound(lscb_num);
            if (itr == by_blk_num.end()) return lscb_info;

            while (itr != by_blk_num.end()) {
                if ((*itr)->is_stable && ctrl.fetch_block_state_by_id((*itr)->block_id)) {
                    auto lscb = ctrl.fetch_block_state_by_number(ctrl.last_stable_checkpoint_block_num());

                    auto head_checkpoint_schedule = ctrl.fetch_block_state_by_id(
                            (*itr)->block_id)->active_schedule;

                    producer_schedule_type current_schedule;
                    producer_schedule_type new_schedule;

                    if (lscb_num == 0) {
                        auto const& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                        if (ucb == 0) {
                            current_schedule = ctrl.initial_schedule();
                            new_schedule = ctrl.initial_schedule();
                        } else {
                            auto bs = ctrl.fetch_block_state_by_number(ucb);
                            if (!bs) return lscb_info;
                            current_schedule = bs->active_schedule;
                            new_schedule = bs->pending_schedule;
                        }
                    } else if (lscb) {
                        current_schedule = lscb->active_schedule;
                        new_schedule = lscb->pending_schedule;
                    } else {
                        return lscb_info;
                    }

                    if ((*itr)->is_stable
                        && (head_checkpoint_schedule == current_schedule || head_checkpoint_schedule == new_schedule)) {
                        lscb_info = block_info_type{(*itr)->block_id};
                    }
                }
                ++itr;
            }
            return lscb_info;
        }

        vector<pbft_checkpoint> pbft_database::generate_and_add_pbft_checkpoint() {
            auto checkpoint = [&](const block_num_type &in) {
                auto const& ucb = ctrl.get_upgrade_properties().upgrade_complete_block_num;
                if (!ctrl.is_pbft_enabled()) return false;
                if (in <= ucb) return false;
                auto watermarks = get_updated_watermarks();
                return in == ucb + 1 // checkpoint on first pbft block;
                       || in % 100 == 1 // checkpoint on every 100 block;
                       || std::find(watermarks.begin(), watermarks.end(), in) != watermarks.end(); // checkpoint on bp schedule change;
            };

            auto new_pc = vector<pbft_checkpoint>{};
            new_pc.reserve(ctrl.my_signature_providers().size());
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
                                auto p_itr = find_if(checkpoints.begin(), checkpoints.end(),
                                        [&](const pbft_checkpoint &ext) { return ext.common.sender == my_sp.first; });
                                if (p_itr != checkpoints.end() && !(*c_itr)->is_stable) pending_checkpoint_block_num[i] = true; //retry sending at this time.
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
                            auto uuid = boost::uuids::to_string(uuid_generator());
                            pbft_checkpoint cp;
                            cp.common.uuid=uuid; cp.block_info={bs->id}; cp.common.sender=my_sp.first; cp.common.chain_id=chain_id;
                            cp.sender_signature = my_sp.second(cp.digest());
                            if (!bnum_and_retry.second) { //first time sending this checkpoint
                                add_pbft_checkpoint(cp);
                            }
                            new_pc.emplace_back(cp);
                        }
                    }
                }
            } else if (lscb_num > 0) { //retry sending my lscb
                for (auto const &my_sp : ctrl.my_signature_providers()) {
                    auto uuid = boost::uuids::to_string(uuid_generator());
                    pbft_checkpoint cp;
                    cp.common.uuid=uuid; cp.block_info={ctrl.last_stable_checkpoint_block_id()}; cp.common.sender=my_sp.first; cp.common.chain_id=chain_id;
                    cp.sender_signature = my_sp.second(cp.digest());
                    new_pc.emplace_back(cp);
                }
            }
            return new_pc;
        }

        void pbft_database::add_pbft_checkpoint(pbft_checkpoint &cp) {

            if (!is_valid_checkpoint(cp)) return;

            auto cp_block_state = ctrl.fetch_block_state_by_id(cp.block_info.block_id);
            if (!cp_block_state) return;
            auto active_bps = cp_block_state->active_schedule.producers;
            auto checkpoint_count = count_if(active_bps.begin(), active_bps.end(), [&](const producer_key &p) {
                return p.block_signing_key == cp.common.sender;
            });
            if (checkpoint_count == 0) return;

            auto &by_block = checkpoint_index.get<by_block_id>();
            auto itr = by_block.find(cp.block_info.block_id);
            if (itr == by_block.end()) {
                auto cs = pbft_checkpoint_state{cp.block_info.block_id, cp.block_info.block_num(), .checkpoints={cp}};
                auto csp = make_shared<pbft_checkpoint_state>(cs);
                checkpoint_index.insert(csp);
                itr = by_block.find(cp.block_info.block_id);
            } else {
                auto csp = (*itr);
                auto checkpoints = csp->checkpoints;
                auto p_itr = find_if(checkpoints.begin(), checkpoints.end(),
                        [&](const pbft_checkpoint &existed) { return existed.common.sender == cp.common.sender; });
                if (p_itr == checkpoints.end()) {
                    by_block.modify(itr, [&](const pbft_checkpoint_state_ptr &pcp) {
                        csp->checkpoints.emplace_back(cp);
                    });
                }
            }

            auto csp = (*itr);
            auto threshold = active_bps.size() * 2 / 3 + 1;
            if (csp->checkpoints.size() >= threshold && !csp->is_stable) {
                auto cp_count = 0;

                for (auto const &sp: active_bps) {
                    for (auto const &pp: csp->checkpoints) {
                        if (sp.block_signing_key == pp.common.sender) cp_count += 1;
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
                emit(pbft_outgoing_checkpoint, cp);
            }
        }

        void pbft_database::checkpoint_local() {
            auto lscb_info = cal_pending_stable_checkpoint();
            auto lscb_num = ctrl.last_stable_checkpoint_block_num();
            auto pending_num = lscb_info.block_num();
            auto pending_id = lscb_info.block_id;
            if (pending_num > lscb_num) {
                ctrl.set_pbft_latest_checkpoint(pending_id);
                if (ctrl.last_irreversible_block_num() < pending_num) ctrl.pbft_commit_local(pending_id);
                auto const &by_block_id_index = pbft_state_index.get<by_block_id>();
                auto pitr = by_block_id_index.find(pending_id);
                if (pitr != by_block_id_index.end()) {
                    prune(*pitr);
                }
            }
            auto &bni = checkpoint_index.get<by_num>();
            auto oldest = bni.begin();
            if (oldest != bni.end() && lscb_num - (*oldest)->block_num > 10000) {
                auto it = bni.lower_bound(lscb_num - 10000);
                if (it != bni.end() && (*it)->is_stable) {
                    prune_checkpoints(*it);
                }
            }
        }

        bool pbft_database::is_valid_checkpoint(const pbft_checkpoint &cp) {
            if (!(is_valid_pbft_message(cp.common) && cp.is_signature_valid())) return false;

            if (cp.block_info.block_num() > ctrl.head_block_num()
                || cp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num()
                || !cp.is_signature_valid())
                return false;
            auto bs = ctrl.fetch_block_state_by_id(cp.block_info.block_id);
            if (bs) {
                auto active_bps = bs->active_schedule.producers;
                for (auto const &bp: active_bps) {
                    if (bp.block_signing_key == cp.common.sender) return true;
                }
            }
            return false;
        }

        bool pbft_database::is_valid_stable_checkpoint(const pbft_stable_checkpoint &scp) {
            if (scp.block_info.block_num() <= ctrl.last_stable_checkpoint_block_num())
                // the stable checkpoint is way behind lib, no way getting the block state,
                // it will not be applied nor saved, thus considered safe.
                return true;

            auto valid = true;
            for (auto const &c: scp.checkpoints) {
                valid = valid && is_valid_checkpoint(c) && c.block_info == scp.block_info;
                if (!valid) return false;
            }

            auto bs = ctrl.fetch_block_state_by_number(scp.block_info.block_num());
            if (bs) {
                auto as = bs->active_schedule;
                auto cp_count = 0;
                for (auto const &sp: as.producers) {
                    for (auto const &v: scp.checkpoints) {
                        if (sp.block_signing_key == v.common.sender) cp_count += 1;
                    }
                }
                valid = valid && cp_count >= as.producers.size() * 2 / 3 + 1;
            } else {
                return false;
            }
            return valid;
        }

        bool pbft_database::is_valid_pbft_message(const pbft_message_common &common) {
            return common.chain_id == chain_id;
        }

        bool pbft_database::should_send_pbft_msg() {

            auto my_sp = ctrl.my_signature_providers();
            auto schedules = get_updated_fork_schedules();
            for (auto const &bp: schedules) {
                for (auto const &my: my_sp) {
                    if (bp.first == my.first) return true;
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

            auto bs = ctrl.fetch_block_state_by_number(num);
            if (!bs) return ctrl.initial_schedule();

            if (bs->pending_schedule.producers.empty()) return bs->active_schedule;
            return bs->pending_schedule;
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
                    auto pruned_num = *max_element(removed.begin(), removed.end());
                    for (auto itr = fork_schedules.begin(); itr != fork_schedules.end();) {
                        if ((*itr).second <= pruned_num) {
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
            auto epitr = by_num_index.upper_bound( num );
            while( pitr != epitr ) {
                if (pitr != by_num_index.end() && (*pitr)) results.emplace_back(*(*pitr));
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

        void pbft_database::prune_checkpoints(const pbft_checkpoint_state_ptr &h) {
            auto num = h->block_num;

            auto &by_bn = checkpoint_index.get<by_num>();
            auto bni = by_bn.begin();
            while (bni != by_bn.end() && (*bni)->block_num < num) {
                prune_checkpoints(*bni);
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