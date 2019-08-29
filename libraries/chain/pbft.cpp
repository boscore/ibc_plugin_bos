#include <eosio/chain/pbft.hpp>
#include <fc/io/fstream.hpp>
#include <fstream>

namespace eosio {
    namespace chain {

        pbft_controller::pbft_controller(controller& ctrl) :
        pbft_db(ctrl),
        state_machine(pbft_db) {
            state_machine.set_current(std::make_shared<psm_committed_state>(state_machine, pbft_db));
            state_machine.set_current_view(pbft_db.get_current_view());
            state_machine.set_target_view(state_machine.get_current_view() + 1);
            ilog("current view: ${cv}", ("cv", pbft_db.get_current_view()));
        }

        pbft_controller::~pbft_controller() = default;

        void pbft_controller::maybe_pbft_prepare() {
            if (!pbft_db.should_send_pbft_msg()) return;
            state_machine.send_prepare();
        }

        void pbft_controller::maybe_pbft_commit() {
            if (!pbft_db.should_send_pbft_msg()) return;
            state_machine.send_commit();
        }

        void pbft_controller::maybe_pbft_view_change() {
            if (!pbft_db.should_send_pbft_msg()) return;

            if (view_change_timeout != pbft_db.get_view_change_timeout()) {
                ///if there is a change in global states, update timeout and reset timer.
                view_change_timeout = pbft_db.get_view_change_timeout();
                state_machine.set_view_change_timer(0);
            }

            if (state_machine.get_view_change_timer() <= view_change_timeout) {
                if (!state_machine.get_view_change_cache().empty()) {
                    pbft_db.generate_and_add_pbft_view_change(state_machine.get_view_change_cache());
                }
                state_machine.set_view_change_timer(state_machine.get_view_change_timer() + 1);
            } else {
                state_machine.set_view_change_timer(0);
                state_machine.send_view_change();
            }
        }

        void pbft_controller::maybe_pbft_checkpoint() {
            if (!pbft_db.should_send_pbft_msg()) return;
            state_machine.send_checkpoint();
            pbft_db.checkpoint_local();
        }

        void pbft_controller::on_pbft_prepare(const pbft_metadata_ptr<pbft_prepare>& p) {
            state_machine.on_prepare(p);
        }

        void pbft_controller::on_pbft_commit(const pbft_metadata_ptr<pbft_commit>& c) {
            state_machine.on_commit(c);
        }

        void pbft_controller::on_pbft_view_change(const pbft_metadata_ptr<pbft_view_change>& vc) {
            state_machine.on_view_change(vc);
        }

        void pbft_controller::on_pbft_new_view(const pbft_metadata_ptr<pbft_new_view>& nv) {
            state_machine.on_new_view(nv);
        }

        void pbft_controller::on_pbft_checkpoint(const pbft_metadata_ptr<pbft_checkpoint>& cp) {
            if (!pbft_db.is_valid_checkpoint(cp->msg, cp->sender_key)) return;
            pbft_db.add_pbft_checkpoint(cp->msg, cp->sender_key);
            pbft_db.checkpoint_local();
        }

        psm_state::psm_state(psm_machine& m, pbft_database& pbft_db) : m(m), pbft_db(pbft_db){}
        psm_state::~psm_state() = default;

        psm_machine::psm_machine(pbft_database& pbft_db) : pbft_db(pbft_db) {}

        psm_machine::~psm_machine() = default;

        void psm_machine::on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) {
            current->on_prepare(e);
        }

        void psm_machine::send_prepare() {
            current->send_prepare();
        }

        void psm_machine::on_commit(const pbft_metadata_ptr<pbft_commit>& e) {
            current->on_commit(e);
        }

        void psm_machine::send_commit() {
            current->send_commit();
        }

        void psm_machine::on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) {
            current->on_view_change(e);
        }

        void psm_machine::send_view_change() {
            current->send_view_change();
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
                transit_to_new_view(e);
            } catch(...) {
                elog("apply new view failed, waiting for next round.. ${nv} ", ("nv", e->msg));
            }
        }

        void psm_machine::manually_set_current_view(uint32_t cv) {
            set_current_view(cv);
            pbft_db.set_current_view(cv);
            set_target_view(cv + 1);
            transit_to_view_change_state();
        }

        /**\
         *
         * psm_prepared_state
         */

        psm_prepared_state::psm_prepared_state(psm_machine& m, pbft_database& pbft_db) : psm_state(m, pbft_db) {pending_commit_local = false;}
        psm_prepared_state::~psm_prepared_state() = default;

        void psm_prepared_state::maybe_transit_to_committed() {

            //`pending_commit_local` is used to mark committed local status in psm machine;
            //`pbft_db.pending_pbft_lib()` is used to mark commit local status in controller;
            // following logic is implemented to resolve async problem during lib committing;

            if (pbft_db.should_committed() && !pending_commit_local) {
                pbft_db.commit_local();
                pending_commit_local = true;
            }

            if (pending_commit_local && !pbft_db.pending_pbft_lib()) {
                m.transit_to_committed_state(false);
            }
        }

        void psm_prepared_state::on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) {
            //ignore
        }

        void psm_prepared_state::send_prepare() {
            //retry
            if (m.get_prepare_cache().empty()) return;
            m.do_send_prepare();
        }

        void psm_prepared_state::on_commit(const pbft_metadata_ptr<pbft_commit>& e) {

            if (e->msg.view < m.get_current_view()) return;
            if (!pbft_db.is_valid_commit(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_commit(e->msg, e->sender_key);
            maybe_transit_to_committed();
        }

        void psm_prepared_state::send_commit() {

            m.do_send_commit();
            maybe_transit_to_committed();
        }

        void psm_prepared_state::on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) {

            if (e->msg.target_view <= m.get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);
            m.maybe_view_change();
        }

        void psm_prepared_state::send_view_change() {
            m.transit_to_view_change_state();
        }


        psm_committed_state::psm_committed_state(psm_machine& m, pbft_database& pbft_db) : psm_state(m, pbft_db) {}
        psm_committed_state::~psm_committed_state() = default;

        /**
         * psm_committed_state
         */
        void psm_committed_state::on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) {
            //validate
            if (e->msg.view < m.get_current_view()) return;
            if (!pbft_db.is_valid_prepare(e->msg, e->sender_key)) return;

            //do action add prepare
            pbft_db.add_pbft_prepare(e->msg, e->sender_key);
            //if prepare >= n-f, transit to prepared
            if (pbft_db.should_prepared()) m.transit_to_prepared_state();
        }

        void psm_committed_state::send_prepare() {

            m.do_send_prepare();
            //if prepare >= n-f, transit to prepared
            if (pbft_db.should_prepared()) m.transit_to_prepared_state();
        }

        void psm_committed_state::on_commit(const pbft_metadata_ptr<pbft_commit>& e) {

            if (e->msg.view < m.get_current_view()) return;
            if (!pbft_db.is_valid_commit(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_commit(e->msg, e->sender_key);
        }

        void psm_committed_state::send_commit() {

            if (m.get_commit_cache().empty()) return;
            m.do_send_commit();
        }

        void psm_committed_state::on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) {

            if (e->msg.target_view <= m.get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);
            m.maybe_view_change();
        }

        void psm_committed_state::send_view_change() {
            m.transit_to_view_change_state();
        }

        psm_view_change_state::psm_view_change_state(psm_machine& m, pbft_database& pbft_db) : psm_state(m, pbft_db) {}
        psm_view_change_state::~psm_view_change_state() = default;
        /**
         * psm_view_change_state
         */
        void psm_view_change_state::on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) {
            //ignore;
        }

        void psm_view_change_state::send_prepare() {
            //ignore;
        }

        void psm_view_change_state::on_commit(const pbft_metadata_ptr<pbft_commit>& e) {
            //ignore;
        }

        void psm_view_change_state::send_commit() {
            //ignore;
        }

        void psm_view_change_state::on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) {

            if (m.maybe_stop_view_change()) return;

            if (e->msg.target_view <= m.get_current_view()) return;
            if (!pbft_db.is_valid_view_change(e->msg, e->sender_key)) return;

            pbft_db.add_pbft_view_change(e->msg, e->sender_key);
            m.maybe_new_view();
        }

        void psm_view_change_state::send_view_change() {

            if (m.maybe_stop_view_change()) return;

            m.do_send_view_change();
            m.maybe_new_view();
        }

        void psm_machine::transit_to_committed_state(bool to_new_view) {

            if (!to_new_view) {
                auto nv = pbft_db.get_committed_view();
                if (nv > get_current_view()) {
                    set_current_view(nv);
                    pbft_db.set_current_view(nv);
                }
                set_target_view(get_current_view() + 1);
            }

            set_prepare_cache(pbft_prepare());
            do_send_prepare();

            set_view_change_cache(pbft_view_change());
            set_view_change_timer(0);

            set_current(std::make_shared<psm_committed_state>(*this, pbft_db));
            if (!get_prepare_cache().empty()) {
                emit(pbft_transit_to_committed, true);
            }
        }

        void psm_machine::transit_to_prepared_state() {

            set_commit_cache(pbft_commit());
            do_send_commit();

            set_view_change_cache(pbft_view_change());

            set_current(std::make_shared<psm_prepared_state>(*this, pbft_db));
            emit(pbft_transit_to_prepared, true);
        }

        void psm_machine::transit_to_view_change_state() {

            set_commit_cache(pbft_commit());
            set_prepare_cache(pbft_prepare());

            set_view_change_timer(0);
            set_target_view_retries(0);

            set_current(std::make_shared<psm_view_change_state>(*this, pbft_db));
            if (pbft_db.should_send_pbft_msg()) {
                do_send_view_change();
                auto nv = maybe_new_view();
                if (nv) return;
            }
        }

        void psm_machine::maybe_view_change() {
            //if received >= f+1 view_change on some view, transit to view_change and send view change
            auto new_view = pbft_db.should_view_change();
            if (new_view > 0 && new_view > get_current_view()) {
                set_target_view(new_view);
                transit_to_view_change_state();
            }
        }

        bool psm_machine::maybe_stop_view_change() {
            //skip from view change state if my lib is higher than my view change state height.
            auto vc = get_view_change_cache();
            if (!vc.empty() && pbft_db.should_stop_view_change(vc)) {
                transit_to_committed_state(false);
                return true;
            }
            return false;
        }

        bool psm_machine::maybe_new_view() {
            //if view_change >= n-f, calculate next primary, send new view if is primary
            auto nv = get_target_view();
            auto pk = pbft_db.get_new_view_primary_key(nv);
            if (pbft_db.should_new_view(nv) && pbft_db.has_new_primary(pk)) {

                set_view_changed_certificate(pbft_db.generate_view_changed_certificate(nv));

                auto new_view = pbft_db.get_proposed_new_view_num();
                if (new_view != nv) return false;

                auto nv_msg = pbft_db.generate_pbft_new_view(
                        get_view_changed_certificate(),
                        new_view);

                if (nv_msg.empty()) return false;
                emit(pbft_outgoing_new_view, std::make_shared<pbft_new_view>(nv_msg));

                try {
                    transit_to_new_view(std::make_shared<pbft_message_metadata<pbft_new_view>>(nv_msg, pbft_db.get_chain_id()));
                    return true;
                } catch(const fc::exception& ex) {
                    elog("apply new view failed, waiting for next round.. ${nv} ", ("nv", nv_msg));
                }
            }
            return false;
        }

        void psm_machine::transit_to_new_view(const pbft_metadata_ptr<pbft_new_view>& e) {

            set_current_view(e->msg.new_view);
            pbft_db.set_current_view(e->msg.new_view);
            set_target_view(e->msg.new_view + 1);

            set_prepare_cache(pbft_prepare());

            set_view_change_timer(0);
            set_target_view_retries(0);

            pbft_db.cleanup_on_new_view();

            if (!e->msg.committed_certs.empty()) {
                auto committed_certs = e->msg.committed_certs;
                std::sort(committed_certs.begin(), committed_certs.end());
                for (const auto& cc :committed_certs) {
                    pbft_db.mark_as_committed(cc.block_info.block_id);
                }
            }

            if (!e->msg.prepared_cert.prepares.empty()) {
                pbft_db.mark_as_prepared(e->msg.prepared_cert.block_info.block_id);
                if (pbft_db.should_prepared()) {
                    transit_to_prepared_state();
                    return;
                }
            }

            if (pbft_db.should_committed()) {
                pbft_db.commit_local();
            }
            transit_to_committed_state(true);
        }

        void psm_machine::do_send_prepare() {
            auto prepares = pbft_db.generate_and_add_pbft_prepare(get_prepare_cache());
            if (!prepares.empty()) {
                for (const auto& p: prepares) {
                    emit(pbft_outgoing_prepare, std::make_shared<pbft_prepare>(p));
                }
                set_prepare_cache(prepares.front());
            }
        }

        void psm_machine::do_send_commit() {
            auto commits = pbft_db.generate_and_add_pbft_commit(get_commit_cache());

            if (!commits.empty()) {
                for (const auto& c: commits) {
                    emit(pbft_outgoing_commit, std::make_shared<pbft_commit>(c));
                }
                set_commit_cache(commits.front());
            }
        }

        void psm_machine::do_send_view_change() {

            auto reset_view_change_state = [&]() {
                set_view_change_cache(pbft_view_change());
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

            auto view_changes = pbft_db.generate_and_add_pbft_view_change(
                    get_view_change_cache(),
                    get_prepared_certificate(),
                    get_committed_certificate(),
                    get_target_view());

            if (!view_changes.empty()) {
                for (const auto& vc : view_changes) {
                    emit(pbft_outgoing_view_change, std::make_shared<pbft_view_change>(vc));
                }
                set_view_change_cache(view_changes.front());
            }
        }

        void psm_machine::send_checkpoint() {
            auto checkpoints = pbft_db.generate_and_add_pbft_checkpoint();
            for (const auto& cp: checkpoints) {
                emit(pbft_outgoing_checkpoint, std::make_shared<pbft_checkpoint>(cp));
            }
        }

        template<typename Signal, typename Arg>
        void psm_machine::emit(const Signal& s, Arg&& a) {
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