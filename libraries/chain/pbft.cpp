#include <utility>

#include <eosio/chain/pbft.hpp>
#include <fc/io/fstream.hpp>
#include <fstream>

namespace eosio {
    namespace chain {

        pbft_controller::pbft_controller(controller &ctrl) : 
        pbft_db(ctrl), 
        state_machine(new psm_machine(pbft_db)) {
            datadir = ctrl.state_dir();

            if (!fc::is_directory(datadir))
                fc::create_directories(datadir);

            auto pbft_db_dat = datadir / config::pbftdb_filename;
            if (fc::exists(pbft_db_dat)) {
                string content;
                fc::read_file_contents(pbft_db_dat, content);

                fc::datastream<const char *> ds(content.data(), content.size());
                uint32_t current_view;
                fc::raw::unpack(ds, current_view);
                state_machine->set_current_view(current_view);
                state_machine->set_target_view(state_machine->get_current_view() + 1);
                ilog("current view: ${cv}", ("cv", current_view));
            }

            fc::remove(pbft_db_dat);
        }

        pbft_controller::~pbft_controller() {
            fc::path pbft_db_dat = datadir / config::pbftdb_filename;
            std::ofstream out(pbft_db_dat.generic_string().c_str(),
                              std::ios::out | std::ios::binary | std::ofstream::trunc);

            uint32_t current_view = state_machine->get_current_view();
            fc::raw::pack(out, current_view);
        }

        void pbft_controller::maybe_pbft_prepare() {
            if (!pbft_db.should_send_pbft_msg()) return;
            state_machine->send_prepare();
        }

        void pbft_controller::maybe_pbft_commit() {
            if (!pbft_db.should_send_pbft_msg()) return;
            state_machine->send_commit();
        }

        void pbft_controller::maybe_pbft_view_change() {
            if (!pbft_db.should_send_pbft_msg()) return;
            if (state_machine->get_view_change_timer() <= view_change_timeout) {
                if (!state_machine->get_view_changes_cache().empty()) {
                    pbft_db.send_and_add_pbft_view_change(state_machine->get_view_changes_cache());
                }
                state_machine->set_view_change_timer(state_machine->get_view_change_timer() + 1);
            } else {
                state_machine->set_view_change_timer(0);
                state_machine->send_view_change();
            }
        }

        void pbft_controller::maybe_pbft_checkpoint() {
            if (!pbft_db.should_send_pbft_msg()) return;
            pbft_db.send_pbft_checkpoint();
            pbft_db.checkpoint_local();
        }

        void pbft_controller::on_pbft_prepare(const pbft_metadata_ptr<pbft_prepare>& p) {
            state_machine->on_prepare(p);
        }

        void pbft_controller::on_pbft_commit(const pbft_metadata_ptr<pbft_commit>& c) {
            state_machine->on_commit(c);
        }

        void pbft_controller::on_pbft_view_change(const pbft_metadata_ptr<pbft_view_change>& vc) {
            state_machine->on_view_change(vc);
        }

        void pbft_controller::on_pbft_new_view(const pbft_metadata_ptr<pbft_new_view>& nv) {
            state_machine->on_new_view(nv);
        }

        void pbft_controller::on_pbft_checkpoint(const pbft_metadata_ptr<pbft_checkpoint> &cp) {
            if (!pbft_db.is_valid_checkpoint(cp->msg, cp->sender_key)) return;
            pbft_db.add_pbft_checkpoint(cp->msg, cp->sender_key);
            pbft_db.checkpoint_local();
        }

        psm_state::psm_state() = default;
        psm_state::~psm_state() = default;

        psm_machine::psm_machine(pbft_database &pbft_db) : pbft_db(pbft_db) {
            set_current(std::make_shared<psm_committed_state>());

            set_prepares_cache(pbft_prepare());
            set_commits_cache(pbft_commit());
            set_view_changes_cache(pbft_view_change());

            set_prepared_certificate(pbft_prepared_certificate{});
            set_committed_certificate(vector<pbft_committed_certificate>{});
            set_view_changed_certificate(pbft_view_changed_certificate{});

            view_change_timer = 0;
            target_view_retries = 0;
            current_view = 0;
            target_view = current_view + 1;
        }

        psm_machine::~psm_machine() = default;

        void psm_machine::on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) {
            current->on_prepare(shared_from_this(), e, pbft_db);
        }

        void psm_machine::send_prepare() {
            current->send_prepare(shared_from_this(), pbft_db);
        }

        void psm_machine::on_commit(const pbft_metadata_ptr<pbft_commit>& e) {
            current->on_commit(shared_from_this(), e, pbft_db);
        }

        void psm_machine::send_commit() {
            current->send_commit(shared_from_this(), pbft_db);
        }

        void psm_machine::on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) {
            current->on_view_change(shared_from_this(), e, pbft_db);
        }

        void psm_machine::send_view_change() {
            current->send_view_change(shared_from_this(), pbft_db);
        }

        void psm_machine::on_new_view(const pbft_metadata_ptr<pbft_new_view>& e) {
            if (e->msg.new_view <= get_current_view()) return;

            try {
                pbft_db.validate_new_view(e->msg, e->sender_key);
            } catch (const fc::exception& ex) {
                elog("bad new view, ${s} ", ("s",ex.to_string()));
                return;
            }

            try {
                transit_to_new_view(e, current);
            } catch(...) {
                elog("apply new view failed, waiting for next round.. ${nv} ", ("nv", e->msg));
            }
        }

        void psm_machine::manually_set_current_view(const uint32_t &cv) {
            set_current_view(cv);
            set_target_view(cv + 1);
            transit_to_view_change_state(current);
        }

        /**
         * psm_prepared_state
         */

        psm_prepared_state::psm_prepared_state() {pending_commit_local = false;}
        psm_prepared_state::~psm_prepared_state() = default;

        void psm_prepared_state::on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) {
            //ignore
        }

        void psm_prepared_state::send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) {
            //retry
            if (m->get_prepares_cache().empty()) return;

            pbft_db.send_and_add_pbft_prepare(m->get_prepares_cache(), m->get_current_view());
        }

        void psm_prepared_state::on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) {

            if (e->msg.view < m->get_current_view()) return;
            if (!pbft_db.is_valid_commit(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_commit(e->msg, e->sender_key);

            //`pending_commit_local` is used to mark committed local status in psm machine;
            //`pbft_db.pending_pbft_lib()` is used to mark commit local status in controller;
            // following logic is implemented to resolve async problem during lib committing;

            if (pbft_db.should_committed() && !pending_commit_local) {
                pbft_db.commit_local();
                pending_commit_local = true;
            }

            if (pending_commit_local && !pbft_db.pending_pbft_lib()) {
                pbft_db.send_pbft_checkpoint();
                pbft_db.checkpoint_local();
                m->transit_to_committed_state(shared_from_this(), false);
            }
        }

        void psm_prepared_state::send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) {
            auto commits = pbft_db.send_and_add_pbft_commit(m->get_commits_cache(), m->get_current_view());

            if (!commits.empty()) {
                m->set_commits_cache(commits);
            }

            if (pbft_db.should_committed() && !pending_commit_local) {
                pbft_db.commit_local();
                pending_commit_local = true;
            }

            if (pending_commit_local && !pbft_db.pending_pbft_lib()) {
                pbft_db.send_pbft_checkpoint();
                pbft_db.checkpoint_local();
                m->transit_to_committed_state(shared_from_this(), false);
            }
        }

        void psm_prepared_state::on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) {

            if (e->msg.target_view <= m->get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);

            //if received >= f+1 view_change on some view, transit to view_change and send view change
            auto target_view = pbft_db.should_view_change();
            if (target_view > 0 && target_view > m->get_current_view()) {
                m->set_target_view(target_view);
                m->transit_to_view_change_state(shared_from_this());
            }
        }

        void psm_prepared_state::send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) {
            m->transit_to_view_change_state(shared_from_this());
        }


        psm_committed_state::psm_committed_state() = default;
        psm_committed_state::~psm_committed_state() = default;

        /**
         * psm_committed_state
         */
        void psm_committed_state::on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) {
            //validate
            if (e->msg.view < m->get_current_view()) return;
            if (!pbft_db.is_valid_prepare(e->msg, e->sender_key)) return;

            //do action add prepare
            pbft_db.add_pbft_prepare(e->msg, e->sender_key);

            //if prepare >= 2f+1, transit to prepared
            if (pbft_db.should_prepared()) m->transit_to_prepared_state(shared_from_this());
        }

        void psm_committed_state::send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) {

            auto prepares = pbft_db.send_and_add_pbft_prepare(m->get_prepares_cache(), m->get_current_view());

            if (!prepares.empty()) {
                m->set_prepares_cache(prepares);
            }

            //if prepare >= 2f+1, transit to prepared
            if (pbft_db.should_prepared()) m->transit_to_prepared_state(shared_from_this());
        }

        void psm_committed_state::on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) {

            if (e->msg.view < m->get_current_view()) return;
            if (!pbft_db.is_valid_commit(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_commit(e->msg, e->sender_key);
        }

        void psm_committed_state::send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) {

            if (m->get_commits_cache().empty()) return;
            pbft_db.send_and_add_pbft_commit(m->get_commits_cache(), m->get_current_view());

        }

        void psm_committed_state::on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) {

            if (e->msg.target_view <= m->get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);

            //if received >= f+1 view_change on some view, transit to view_change and send view change
            auto new_view = pbft_db.should_view_change();
            if (new_view > 0 && new_view > m->get_current_view()) {
                m->set_target_view(new_view);
                m->transit_to_view_change_state(shared_from_this());
            }
        }

        void psm_committed_state::send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) {
            m->transit_to_view_change_state(shared_from_this());
        }

        psm_view_change_state::psm_view_change_state() = default;
        psm_view_change_state::~psm_view_change_state() = default;
        /**
         * psm_view_change_state
         */
        void psm_view_change_state::on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) {
            //ignore;
        }

        void psm_view_change_state::send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) {
            //ignore;
        }

        void psm_view_change_state::on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) {
            //ignore;
        }

        void psm_view_change_state::send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) {
            //ignore;
        }

        void psm_view_change_state::on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) {

            //skip from view change state if my lib is higher than my view change state height.
            auto vc = m->get_view_changes_cache();
            if (!vc.empty() && pbft_db.should_stop_view_change(vc)) {
                m->transit_to_committed_state(shared_from_this(), false);
                return;
            }

            if (e->msg.target_view <= m->get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);

            m->maybe_new_view(shared_from_this());
        }

        void psm_view_change_state::send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) {

            //skip from view change state if my lib is higher than my view change state height.
            auto vc = m->get_view_changes_cache();
            if (!vc.empty() && pbft_db.should_stop_view_change(vc)) {
                m->transit_to_committed_state(shared_from_this(), false);
                return;
            }

            m->do_send_view_change();

            m->maybe_new_view(shared_from_this());
        }

        void psm_machine::transit_to_committed_state(const psm_state_ptr& s, bool to_new_view) {

            if (!to_new_view) {
                auto nv = pbft_db.get_committed_view();
                if (nv > get_current_view()) set_current_view(nv);
                set_target_view(get_current_view() + 1);
            }

            auto prepares = pbft_db.send_and_add_pbft_prepare(pbft_prepare(), get_current_view());
            set_prepares_cache(prepares);
            //TODO: reset prepare timer;

            set_view_changes_cache(pbft_view_change());
            set_view_change_timer(0);

            set_current(std::make_shared<psm_committed_state>());
        }

        void psm_machine::transit_to_prepared_state(const psm_state_ptr& s) {

            auto commits = pbft_db.send_and_add_pbft_commit(pbft_commit(), get_current_view());
            set_commits_cache(commits);
            //TODO: reset commit timer;

            set_view_changes_cache(pbft_view_change());

            set_current(std::make_shared<psm_prepared_state>());
        }

        void psm_machine::transit_to_view_change_state(const psm_state_ptr& s) {

            set_commits_cache(pbft_commit());
            set_prepares_cache(pbft_prepare());

            set_view_change_timer(0);
            set_target_view_retries(0);

            set_current(std::make_shared<psm_view_change_state>());
            if (pbft_db.should_send_pbft_msg()) {
                do_send_view_change();
                auto nv = maybe_new_view(s);
                if (nv) return;
            }
        }

        bool psm_machine::maybe_new_view(const psm_state_ptr &s) {
            //if view_change >= 2f+1, calculate next primary, send new view if is primary
            auto nv = get_target_view();
            auto pk = pbft_db.get_new_view_primary_key(nv);
            if (pbft_db.should_new_view(nv) && pbft_db.has_new_primary(pk)) {

                set_view_changed_certificate(pbft_db.generate_view_changed_certificate(nv));

                auto new_view = pbft_db.get_proposed_new_view_num();
                if (new_view != nv) return false;

                auto nv_msg = pbft_db.send_pbft_new_view(
                        get_view_changed_certificate(),
                        new_view);

                if (nv_msg.empty()) return false;

                try {
                    transit_to_new_view(std::make_shared<pbft_message_metadata<pbft_new_view>>(nv_msg, pbft_db.get_chain_id()), s);
                    return true;
                } catch(const fc::exception& ex) {
                    elog("apply new view failed, waiting for next round.. ${nv} ", ("nv", nv_msg));
                }
            }
            return false;
        }

        void psm_machine::transit_to_new_view(const pbft_metadata_ptr<pbft_new_view>& e, const psm_state_ptr& s) {

            set_current_view(e->msg.new_view);
            set_target_view(e->msg.new_view + 1);

            set_prepares_cache(pbft_prepare());

            set_view_change_timer(0);
            set_target_view_retries(0);

            pbft_db.cleanup_on_new_view();

            if (!e->msg.committed_certs.empty()) {
                auto committed_certs = e->msg.committed_certs;
                std::sort(committed_certs.begin(), committed_certs.end());
                for (auto const &cc :committed_certs) {
                    pbft_db.mark_as_committed(cc.block_info.block_id);
                }
            }

            if (!e->msg.prepared_cert.prepares.empty()) {
                pbft_db.mark_as_prepared(e->msg.prepared_cert.block_info.block_id);
                if (pbft_db.should_prepared()) {
                    transit_to_prepared_state(s);
                    return;
                }
            }

            if (pbft_db.should_committed()) {
                pbft_db.commit_local();
            }
            transit_to_committed_state(s, true);
        }

        void psm_machine::do_send_view_change() {

            auto reset_view_change_state = [&]() {
                set_view_changes_cache(pbft_view_change());
                set_prepared_certificate(pbft_db.generate_prepared_certificate());
                set_committed_certificate(pbft_db.generate_committed_certificate());
            };

            if (get_target_view_retries() < pow(2,get_target_view() - get_current_view() - 1)) {
                if (get_target_view_retries() == 0) reset_view_change_state();
                set_target_view_retries(get_target_view_retries() + 1);
            } else {
                set_target_view_retries(0);
                set_target_view(get_target_view() + 1);
                reset_view_change_state();
            }

            EOS_ASSERT((get_target_view() > get_current_view()), pbft_exception,
                       "target view should be always greater than current view");

            auto view_changes = pbft_db.send_and_add_pbft_view_change(
                    get_view_changes_cache(),
                    get_prepared_certificate(),
                    get_committed_certificate(),
                    get_current_view(),
                    get_target_view());

            if (!view_changes.empty()) {
                set_view_changes_cache(view_changes);
            }
        }

        const pbft_prepare& psm_machine::get_prepares_cache() const {
            return cache.prepares_cache;
        }

        void psm_machine::set_prepares_cache(const pbft_prepare &pcache) {
            cache.prepares_cache = pcache;
        }

        const pbft_commit& psm_machine::get_commits_cache() const {
            return cache.commits_cache;
        }

        void psm_machine::set_commits_cache(const pbft_commit &ccache) {
            cache.commits_cache = ccache;
        }

        const pbft_view_change& psm_machine::get_view_changes_cache() const {
            return cache.view_changes_cache;
        }

        void psm_machine::set_view_changes_cache(const pbft_view_change &vc_cache) {
            cache.view_changes_cache = vc_cache;
        }

        const uint32_t& psm_machine::get_current_view() const {
            return current_view;
        }

        void psm_machine::set_current_view(const uint32_t &cv) {
            current_view = cv;
        }

        const pbft_prepared_certificate& psm_machine::get_prepared_certificate() const {
            return cache.prepared_certificate;
        }

        void psm_machine::set_prepared_certificate(const pbft_prepared_certificate &pcert) {
            cache.prepared_certificate = pcert;
        }

        const vector<pbft_committed_certificate>& psm_machine::get_committed_certificate() const {
            return cache.committed_certificate;
        }

        void psm_machine::set_committed_certificate(const vector<pbft_committed_certificate> &ccert) {
            cache.committed_certificate = ccert;
        }

        const pbft_view_changed_certificate& psm_machine::get_view_changed_certificate() const {
            return cache.view_changed_certificate;
        }

        void psm_machine::set_view_changed_certificate(const pbft_view_changed_certificate &vc_cert) {
            cache.view_changed_certificate = vc_cert;
        }

        const uint32_t& psm_machine::get_target_view_retries() const {
            return target_view_retries;
        }

        void psm_machine::set_target_view_retries(const uint32_t &tv_reties) {
            target_view_retries = tv_reties;
        }

        const uint32_t& psm_machine::get_target_view() const {
            return target_view;
        }

        void psm_machine::set_target_view(const uint32_t &tv) {
            target_view = tv;
        }

        const uint32_t& psm_machine::get_view_change_timer() const {
            return view_change_timer;
        }

        void psm_machine::set_view_change_timer(const uint32_t &vc_timer) {
            view_change_timer = vc_timer;
        }
    }
}