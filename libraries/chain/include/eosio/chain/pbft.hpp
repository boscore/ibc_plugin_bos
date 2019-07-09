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
            pbft_prepare                        prepares_cache;
            pbft_commit                         commits_cache;
            pbft_view_change                    view_changes_cache;
            pbft_prepared_certificate           prepared_certificate;
            vector<pbft_committed_certificate>  committed_certificate;
            pbft_view_changed_certificate       view_changed_certificate;
        };

        class psm_state;
        using psm_state_ptr = std::shared_ptr<psm_state>;

        class psm_machine : public std::enable_shared_from_this<psm_machine> {

        public:
            explicit psm_machine(pbft_database& pbft_db);
            ~psm_machine();

            void set_current(psm_state_ptr s) {
                current = std::move(s);
            }

            psm_state_ptr get_current() {
                return current;
            }

            void on_prepare(const pbft_metadata_ptr<pbft_prepare>& e);
            void on_commit(const pbft_metadata_ptr<pbft_commit>& e);
            void on_view_change(const pbft_metadata_ptr<pbft_view_change>& e);
            void on_new_view(const pbft_metadata_ptr<pbft_new_view>& e);

            void send_prepare();
            void send_commit();
            void send_view_change();

            void transit_to_committed_state(const psm_state_ptr& s, bool to_new_view);
            void transit_to_prepared_state(const psm_state_ptr& s);
            void transit_to_view_change_state(const psm_state_ptr& s);
            void transit_to_new_view(const pbft_metadata_ptr<pbft_new_view>& e, const psm_state_ptr& s);

            void do_send_view_change();
            bool maybe_new_view(const psm_state_ptr& s);

            const pbft_prepare& get_prepares_cache() const;
            void set_prepares_cache(const pbft_prepare &pcache);

            const pbft_commit& get_commits_cache() const;
            void set_commits_cache(const pbft_commit &ccache);

            const pbft_view_change& get_view_changes_cache() const;
            void set_view_changes_cache(const pbft_view_change &vc_cache);

            const uint32_t &get_current_view() const;
            void set_current_view(const uint32_t &cv);

            const pbft_prepared_certificate& get_prepared_certificate() const;
            void set_prepared_certificate(const pbft_prepared_certificate &pcert);

            const vector<pbft_committed_certificate>& get_committed_certificate() const;
            void set_committed_certificate(const vector<pbft_committed_certificate> &ccert);

            const pbft_view_changed_certificate& get_view_changed_certificate() const;
            void set_view_changed_certificate(const pbft_view_changed_certificate &vc_cert);

            const uint32_t& get_target_view_retries() const;
            void set_target_view_retries(const uint32_t &tv_reties);

            const uint32_t& get_target_view() const;
            void set_target_view(const uint32_t &tv);

            const uint32_t& get_view_change_timer() const;
            void set_view_change_timer(const uint32_t &vc_timer);

            void manually_set_current_view(const uint32_t &cv);

        protected:
            psm_cache   cache;
            uint32_t    current_view;
            uint32_t    target_view_retries;
            uint32_t    target_view;
            uint32_t    view_change_timer;

        private:
            psm_state_ptr current;
            pbft_database &pbft_db;
        };

        using psm_machine_ptr = std::shared_ptr<psm_machine>;

        class psm_state : public std::enable_shared_from_this<psm_state> {

        public:
            psm_state();
            ~psm_state();

            virtual void on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) = 0;
            virtual void on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) = 0;
            virtual void on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) = 0;

            virtual void send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) = 0;
            virtual void send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) = 0;
            virtual void send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) = 0;

            virtual const char* get_name() = 0;
            std::shared_ptr<psm_state> get_self()  { return shared_from_this(); };
        };

        class psm_prepared_state final: public psm_state {

        public:
            psm_prepared_state();
            ~psm_prepared_state();

            void on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) override;
            void on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) override;
            void on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) override;

            void send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) override;

            bool pending_commit_local;

            const char* get_name() override { return "{==== PREPARED ====}"; }
        };

        class psm_committed_state final: public psm_state {
        public:
            psm_committed_state();
            ~psm_committed_state();

            void on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) override;
            void on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) override;
            void on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) override;

            void send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) override;

            const char* get_name() override { return "{==== COMMITTED ====}"; }
        };

        class psm_view_change_state final: public psm_state {
        public:
            psm_view_change_state();
            ~psm_view_change_state();

            void on_prepare(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_prepare>& e, pbft_database &pbft_db) override;
            void on_commit(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_commit>& e, pbft_database &pbft_db) override;
            void on_view_change(const psm_machine_ptr& m, const pbft_metadata_ptr<pbft_view_change>& e, pbft_database &pbft_db) override;

            void send_prepare(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_commit(const psm_machine_ptr& m, pbft_database &pbft_db) override;
            void send_view_change(const psm_machine_ptr& m, pbft_database &pbft_db) override;

            const char* get_name() override { return "{==== VIEW CHANGE ====}"; }
        };

        class pbft_controller {
        public:
            explicit pbft_controller(controller& ctrl);
            ~pbft_controller();

            const uint16_t                  view_change_timeout = 6;

            pbft_database                   pbft_db;
            std::shared_ptr<psm_machine>    state_machine;

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
        };
    }
} /// namespace eosio::chain

FC_REFLECT(eosio::chain::pbft_controller, (pbft_db)(state_machine))