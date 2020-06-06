#include <memory>

#include <eosio/pbft_plugin/pbft_plugin.hpp>
#include <boost/asio/steady_timer.hpp>
#include <eosio/chain/global_property_object.hpp>
#include <boost/signals2/connection.hpp>

namespace eosio {
    static appbase::abstract_plugin &_pbft_plugin = app().register_plugin<pbft_plugin>();
    using namespace std;
    using namespace eosio::chain;

    using namespace eosio::chain::plugin_interface;
    using boost::signals2::scoped_connection;

    class pbft_plugin_impl {
    public:
        pbft_plugin_impl()
        :transit_to_committed_channel(app().get_channel<pbft::committed_transition_channel>())
        ,transit_to_prepared_channel(app().get_channel<pbft::prepared_transition_channel>())
        {}

        unique_ptr<boost::asio::steady_timer> prepare_timer;
        unique_ptr<boost::asio::steady_timer> commit_timer;
        unique_ptr<boost::asio::steady_timer> view_change_timer;
        unique_ptr<boost::asio::steady_timer> checkpoint_timer;

        boost::asio::steady_timer::duration prepare_timeout{std::chrono::milliseconds{750}};
        boost::asio::steady_timer::duration commit_timeout{std::chrono::milliseconds{750}};
        boost::asio::steady_timer::duration view_change_check_interval{std::chrono::seconds{5}};
        boost::asio::steady_timer::duration checkpoint_timeout{std::chrono::seconds{25}};

        void prepare_timer_tick();
        void commit_timer_tick();
        void view_change_timer_tick();
        void checkpoint_timer_tick();

        fc::optional<scoped_connection>                             pbft_transit_to_committed_connection;
        pbft::committed_transition_channel::channel_type::handle    transit_to_committed_subscription;
        pbft::committed_transition_channel::channel_type&           transit_to_committed_channel;

        fc::optional<scoped_connection>                             pbft_transit_to_prepared_connection;
        pbft::prepared_transition_channel::channel_type::handle     transit_to_prepared_subscription;
        pbft::prepared_transition_channel::channel_type&            transit_to_prepared_channel;

        void on_committed_transition();
        void on_prepared_transition();

    private:
        bool upgraded = false;
        bool is_replaying();
        bool is_syncing();
        bool pbft_ready();
        bool is_production_paused();
    };

    void pbft_plugin_impl::on_committed_transition() {
        prepare_timer_tick();
    }

    void pbft_plugin_impl::on_prepared_transition() {
        commit_timer_tick();
    }

    void pbft_plugin_impl::prepare_timer_tick() {

        prepare_timer->expires_from_now(prepare_timeout);
        prepare_timer->async_wait([&](boost::system::error_code ec) {
            if ( ec == boost::asio::error::operation_aborted ) {
                return;
            } else if (ec) {
                wlog ("pbft plugin prepare timer tick error: ${m}", ("m", ec.message()));
            } else if (pbft_ready()) {
                chain::pbft_controller& pbft_ctrl = app().get_plugin<chain_plugin>().pbft_ctrl();
                pbft_ctrl.maybe_pbft_prepare();
            }
            prepare_timer_tick();
        });
    }

    void pbft_plugin_impl::commit_timer_tick() {

        commit_timer->expires_from_now(commit_timeout);
        commit_timer->async_wait([&](boost::system::error_code ec) {
            if ( ec == boost::asio::error::operation_aborted ) {
                return;
            } else if (ec) {
                wlog ("pbft plugin commit timer tick error: ${m}", ("m", ec.message()));
            } else if (pbft_ready()) {
                chain::pbft_controller& pbft_ctrl = app().get_plugin<chain_plugin>().pbft_ctrl();
                pbft_ctrl.maybe_pbft_commit();
            }
            commit_timer_tick();
        });
    }

    void pbft_plugin_impl::view_change_timer_tick() {

        try {
            view_change_timer->cancel();
        } catch (boost::system::system_error &e) {
            elog("view change timer cancel error: ${e}", ("e", e.what()));
        }
        view_change_timer->expires_from_now(view_change_check_interval);
        view_change_timer->async_wait([&](boost::system::error_code ec) {
            view_change_timer_tick();
            if (ec) {
                wlog ("pbft plugin view change timer tick error: ${m}", ("m", ec.message()));
            } else if (pbft_ready()) {
                chain::pbft_controller& pbft_ctrl = app().get_plugin<chain_plugin>().pbft_ctrl();
                pbft_ctrl.maybe_pbft_view_change();
            }
        });
    }

    void pbft_plugin_impl::checkpoint_timer_tick() {

        checkpoint_timer->expires_from_now(checkpoint_timeout);
        checkpoint_timer->async_wait([&](boost::system::error_code ec) {
            if ( ec == boost::asio::error::operation_aborted ) {
                return;
            } if (ec) {
                wlog ("pbft plugin checkpoint timer tick error: ${m}", ("m", ec.message()));
            } else if (pbft_ready()) {
                chain::pbft_controller& pbft_ctrl = app().get_plugin<chain_plugin>().pbft_ctrl();
                pbft_ctrl.maybe_pbft_checkpoint();

                chain::controller& ctrl = app().get_plugin<chain_plugin>().chain();
                if ( ctrl.head_block_num() - ctrl.last_stable_checkpoint_block_num() / pbft_ctrl.pbft_db.get_checkpoint_interval() > 1) {
                    //perhaps we need to sync stable checkpoints from other peers
                    app().get_plugin<net_plugin>().maybe_sync_stable_checkpoints();
                }
            }
            checkpoint_timer_tick();
        });
    }

    bool pbft_plugin_impl::is_replaying() {
        return app().get_plugin<chain_plugin>().chain().is_replaying();
    }

    bool pbft_plugin_impl::is_syncing() {
        return app().get_plugin<net_plugin>().is_syncing();
    }

    bool pbft_plugin_impl::is_production_paused() {
        return app().get_plugin<producer_plugin>().paused();
    }

    bool pbft_plugin_impl::pbft_ready() {
        // only trigger pbft related logic if I am in sync and replayed.

        auto& chain = app().get_plugin<chain_plugin>().chain();
        auto enabled = chain.is_pbft_enabled();

        if (enabled && !upgraded) {
            wlog( "\n"
                  "******** BATCH-PBFT ENABLED ********\n"
                  "*                                  *\n"
                  "* --       The blockchain       -- *\n"
                  "* -  has successfully switched   - *\n"
                  "* -     into the new version     - *\n"
                  "* -        Please enjoy a        - *\n"
                  "* -      better performance!     - *\n"
                  "*                                  *\n"
                  "************************************\n" );
            upgraded = true;
        }

        return enabled && !is_syncing() && !is_replaying() && !is_production_paused();
    }

    pbft_plugin::pbft_plugin() : my(new pbft_plugin_impl()) {}

    pbft_plugin::~pbft_plugin() = default;

    void pbft_plugin::set_program_options(options_description &, options_description &cfg) {
    }

    void pbft_plugin::plugin_initialize(const variables_map &options) {
        ilog("Initialize pbft plugin");
        my->prepare_timer = std::make_unique<boost::asio::steady_timer>(app().get_io_service());
        my->commit_timer = std::make_unique<boost::asio::steady_timer>(app().get_io_service());
        my->view_change_timer = std::make_unique<boost::asio::steady_timer>(app().get_io_service());
        my->checkpoint_timer = std::make_unique<boost::asio::steady_timer>(app().get_io_service());
        chain_plug = app().find_plugin<chain_plugin>();
    }

    void pbft_plugin::plugin_startup() {
        my->prepare_timer_tick();
        my->commit_timer_tick();
        my->view_change_timer_tick();
        my->checkpoint_timer_tick();

        my->pbft_transit_to_prepared_connection = app().get_plugin<chain_plugin>().pbft_ctrl().state_machine.pbft_transit_to_prepared.connect( [this]( bool prepared ) {
            my->on_prepared_transition();
        });

        my->pbft_transit_to_committed_connection =  app().get_plugin<chain_plugin>().pbft_ctrl().state_machine.pbft_transit_to_committed.connect( [this]( bool committed ) {
            my->on_committed_transition();
        });
    }

    void pbft_plugin::plugin_shutdown() {}

    pbft_state pbft_plugin::get_pbft_record( const block_id_type& bid ) const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        auto record = pbft_ctrl.pbft_db.get_pbft_state_by_id(bid);
        if (record) return *record;
        return pbft_state();
    }

    vector<pbft_checkpoint_state> pbft_plugin::get_pbft_checkpoints_record(block_num_type bnum) const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        auto records = pbft_ctrl.pbft_db.get_checkpoints_by_num(bnum);
        if (!records.empty()) return records;
        return vector<pbft_checkpoint_state>();
    }
    pbft_view_change_state pbft_plugin::get_view_change_record(pbft_view_type view) const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        auto record = pbft_ctrl.pbft_db.get_view_changes_by_target_view(view);
        if (record) return *record;
        return pbft_view_change_state();
    }

    vector<block_num_type> pbft_plugin::get_watermarks() const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        return pbft_ctrl.pbft_db.get_pbft_watermarks();
    }

    flat_map<public_key_type, uint32_t> pbft_plugin::get_fork_schedules() const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        return pbft_ctrl.pbft_db.get_pbft_fork_schedules();
    }

    const char* pbft_plugin::get_pbft_status() const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        return pbft_ctrl.state_machine.get_current()->get_name();
    }

    block_id_type pbft_plugin::get_pbft_prepared_id() const {
        auto& ctrl = chain_plug->chain();
        return ctrl.get_pbft_prepared();
    }

    block_id_type pbft_plugin::get_pbft_my_prepare_id() const {
        auto& ctrl = chain_plug->chain();
        return ctrl.get_pbft_my_prepare();
    }

    vector<producer_key> pbft_plugin::get_view_change_missing_bps(pbft_view_type view) const {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        auto missing_bps = vector<producer_key>{};
        auto records = get_view_change_record(view);
        if (!records.view_changes.empty() && !records.is_view_changed) {
            auto lscb_bps = pbft_ctrl.pbft_db.lscb_active_producers().producers;
            missing_bps.reserve(lscb_bps.size());
            for (const auto& bp: lscb_bps) {
                auto found = false;
                for (const auto& v: records.view_changes) {
                    if (bp.block_signing_key == v.first) found = true;
                }
                if (!found) missing_bps.emplace_back(bp);
            }
        }
        return missing_bps;
    }

    vector<producer_key> pbft_plugin::get_prepare_missing_bps(const block_id_type& bid) const {
        auto& ctrl = chain_plug->chain();
        auto missing_bps = vector<producer_key>{};
        auto blk = ctrl.fetch_block_state_by_id(bid);
        if (blk) {
            auto records = get_pbft_record(bid);
            if (!records.prepares.empty() && !records.is_prepared) {
                pbft_view_type highest_view;
                for (const auto &p: records.prepares) {
                    if (p.first.first > highest_view) highest_view = p.first.first;
                }
                auto active_bps = blk->active_schedule.producers;;
                missing_bps.reserve(active_bps.size());
                for (const auto &bp: active_bps) {
                    auto found = false;
                    for (const auto &p: records.prepares) {
                        if (p.first.first == highest_view && bp.block_signing_key == p.first.second) {
                            found = true;
                        }
                    }
                    if (!found) missing_bps.emplace_back(bp);
                }
            }

        }
        return missing_bps;
    }

    vector<producer_key> pbft_plugin::get_commit_missing_bps(const block_id_type& bid) const {
        auto& ctrl = chain_plug->chain();
        auto missing_bps = vector<producer_key>{};
        auto blk = ctrl.fetch_block_state_by_id(bid);
        if (blk) {
            auto records = get_pbft_record(bid);
            if (!records.commits.empty() && !records.is_committed) {
                pbft_view_type highest_view;
                for (const auto &p: records.commits) {
                    if (p.first.first > highest_view) highest_view = p.first.first;
                }
                auto active_bps = blk->active_schedule.producers;;
                missing_bps.reserve(active_bps.size());
                for (const auto &bp: active_bps) {
                    auto found = false;
                    for (const auto &c: records.commits) {
                        if (c.first.first == highest_view && bp.block_signing_key == c.first.second) {
                            found = true;
                        }
                    }
                    if (!found) missing_bps.emplace_back(bp);
                }
            }
        }
        return missing_bps;
    }

    void pbft_plugin::set_pbft_current_view(pbft_view_type view) {
        auto& pbft_ctrl = chain_plug->pbft_ctrl();
        //this is used to boost the recovery from a disaster, do not set this unless you have to do so.
        pbft_ctrl.state_machine.manually_set_current_view(view);
    }
}
