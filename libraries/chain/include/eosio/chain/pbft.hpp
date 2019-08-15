#include <utility>

#pragma once

#include <eosio/chain/pbft_database.hpp>
#include <eosio/chain/producer_schedule.hpp>
#include <chrono>

namespace eosio {
    namespace chain {
        using namespace std;
        using namespace fc;

        struct psm_cache {
            pbft_prepare                        prepare_cache = pbft_prepare();
            pbft_commit                         commit_cache = pbft_commit();
            pbft_view_change                    view_change_cache = pbft_view_change();
            pbft_prepared_certificate           prepared_certificate = pbft_prepared_certificate();
            vector<pbft_committed_certificate>  committed_certificate = vector<pbft_committed_certificate>{};
            pbft_view_changed_certificate       view_changed_certificate = pbft_view_changed_certificate();
        };

        class psm_state;
        using psm_state_ptr = std::shared_ptr<psm_state>;

        class psm_machine : public std::enable_shared_from_this<psm_machine> {

        public:
            explicit psm_machine(pbft_database& pbft_db);
            ~psm_machine();

            void set_current(psm_state_ptr s) { current = std::move(s); }

            const psm_state_ptr& get_current() { return current; }

            void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e);
            void on_commit(const pbft_metadata_ptr<pbft_commit>& e);
            void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e);
            void on_new_view(const pbft_metadata_ptr<pbft_new_view>& e);

            void send_prepare();
            void send_commit();
            void send_view_change();
            void send_checkpoint();
            bool maybe_new_view();
            void maybe_view_change();
            bool maybe_stop_view_change();

            void transit_to_committed_state(bool to_new_view);
            void transit_to_prepared_state();
            void transit_to_view_change_state();
            void transit_to_new_view(const pbft_metadata_ptr<pbft_new_view>& e);

            void do_send_prepare();
            void do_send_commit();
            void do_send_view_change();

            const pbft_prepare& get_prepare_cache() const { return cache.prepare_cache; }
            void set_prepare_cache(const pbft_prepare& pcache) { cache.prepare_cache = pcache; }

            const pbft_commit& get_commit_cache() const { return cache.commit_cache; }
            void set_commit_cache(const pbft_commit& ccache) { cache.commit_cache = ccache; }

            const pbft_view_change& get_view_change_cache() const { return cache.view_change_cache; }
            void set_view_change_cache(const pbft_view_change& vc_cache) { cache.view_change_cache = vc_cache; }

            uint32_t get_current_view() const { return current_view; }
            void set_current_view(uint32_t cv) { current_view = cv; }

            const pbft_prepared_certificate& get_prepared_certificate() const { return cache.prepared_certificate; }
            void set_prepared_certificate(const pbft_prepared_certificate& pcert) { cache.prepared_certificate = pcert; }

            const vector<pbft_committed_certificate>& get_committed_certificate() const { return cache.committed_certificate; }
            void set_committed_certificate(const vector<pbft_committed_certificate>& ccert) { cache.committed_certificate = ccert; }

            const pbft_view_changed_certificate& get_view_changed_certificate() const { return cache.view_changed_certificate; }
            void set_view_changed_certificate(const pbft_view_changed_certificate& vc_cert) { cache.view_changed_certificate = vc_cert; }

            uint32_t get_target_view_retries() const { return target_view_retries; }
            void set_target_view_retries(uint32_t tv_reties) { target_view_retries = tv_reties; }

            uint32_t get_target_view() const { return target_view; }
            void set_target_view(uint32_t tv) { target_view = tv; }

            uint32_t get_view_change_timer() const { return view_change_timer; }
            void set_view_change_timer(uint32_t vc_timer) { view_change_timer = vc_timer; }

            void manually_set_current_view(uint32_t cv);

            signal<void(const pbft_prepare_ptr&)> pbft_outgoing_prepare;
            signal<void(const pbft_commit_ptr&)> pbft_outgoing_commit;
            signal<void(const pbft_view_change_ptr&)> pbft_outgoing_view_change;
            signal<void(const pbft_new_view_ptr&)> pbft_outgoing_new_view;
            signal<void(const pbft_checkpoint_ptr&)> pbft_outgoing_checkpoint;
            signal<void(const bool)> pbft_transit_to_committed;
            signal<void(const bool)> pbft_transit_to_prepared;

            template<typename Signal, typename Arg>
            void emit(const Signal& s, Arg&& a);

        protected:
            psm_cache   cache;
            uint32_t    current_view = 0;
            uint32_t    target_view_retries = 0;
            uint32_t    target_view = current_view + 1;
            uint32_t    view_change_timer = 0;

        private:
            psm_state_ptr   current = nullptr;
            pbft_database&  pbft_db;
        };

        using psm_machine_ptr = std::shared_ptr<psm_machine>;

        class psm_state : public std::enable_shared_from_this<psm_state> {

        public:
            psm_state(psm_machine& m, pbft_database& pbft_db);
            ~psm_state();

            virtual void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) = 0;
            virtual void on_commit(const pbft_metadata_ptr<pbft_commit>& e) = 0;
            virtual void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) = 0;

            virtual void send_prepare() = 0;
            virtual void send_commit() = 0;
            virtual void send_view_change() = 0;

            virtual const char* get_name() = 0;
            std::shared_ptr<psm_state> get_self()  { return shared_from_this(); };

        protected:
            psm_machine&    m;
            pbft_database&  pbft_db;
        };

        class psm_prepared_state final: public psm_state {

        public:
            psm_prepared_state(psm_machine& m, pbft_database& pbft_db);
            ~psm_prepared_state();

            void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) override;
            void on_commit(const pbft_metadata_ptr<pbft_commit>& e) override;
            void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) override;

            void send_prepare() override;
            void send_commit() override;
            void send_view_change() override;

            void maybe_transit_to_committed();

            bool pending_commit_local;

            const char* get_name() override { return "{==== PREPARED ====}"; }
        };

        class psm_committed_state final: public psm_state {
        public:
            psm_committed_state(psm_machine& m, pbft_database& pbft_db);
            ~psm_committed_state();

            void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) override;
            void on_commit(const pbft_metadata_ptr<pbft_commit>& e) override;
            void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) override;

            void send_prepare() override;
            void send_commit() override;
            void send_view_change() override;

            const char* get_name() override { return "{==== COMMITTED ====}"; }
        };

        class psm_view_change_state final: public psm_state {
        public:
            psm_view_change_state(psm_machine& m, pbft_database& pbft_db);
            ~psm_view_change_state();

            void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e) override;
            void on_commit(const pbft_metadata_ptr<pbft_commit>& e) override;
            void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e) override;

            void send_prepare() override;
            void send_commit() override;
            void send_view_change() override;

            const char* get_name() override { return "{==== VIEW CHANGE ====}"; }
        };

        class pbft_controller {
        public:
            explicit pbft_controller(controller& ctrl);
            ~pbft_controller();

            pbft_database   pbft_db;
            psm_machine     state_machine;

            void maybe_pbft_prepare();
            void maybe_pbft_commit();
            void maybe_pbft_view_change();
            void maybe_pbft_checkpoint();

            void on_pbft_prepare(const pbft_metadata_ptr<pbft_prepare>& p);
            void on_pbft_commit(const pbft_metadata_ptr<pbft_commit>& c);
            void on_pbft_view_change(const pbft_metadata_ptr<pbft_view_change>& vc);
            void on_pbft_new_view(const pbft_metadata_ptr<pbft_new_view>& nv);
            void on_pbft_checkpoint(const pbft_metadata_ptr<pbft_checkpoint>& cp);

        private:
            fc::path    datadir;
            uint16_t    view_change_timeout = 6;
        };
    }
} /// namespace eosio::chain

FC_REFLECT(eosio::chain::pbft_controller, (pbft_db)(state_machine))