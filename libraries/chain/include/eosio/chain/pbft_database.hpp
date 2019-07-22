#pragma once

#include <eosio/chain/controller.hpp>
#include <eosio/chain/fork_database.hpp>
#include <boost/signals2/signal.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <fc/bitutil.hpp>

namespace eosio {
    namespace chain {
        using boost::multi_index_container;
        using namespace boost::multi_index;
        using namespace std;

        using pbft_view_type = uint32_t;

        constexpr uint16_t pbft_checkpoint_granularity = 100;
        constexpr uint16_t oldest_stable_checkpoint = 10000;

        enum class pbft_message_type : uint8_t {
            prepare,
            commit,
            checkpoint,
            view_change,
            new_view
        };

        struct block_info_type {
            block_id_type   block_id;

            block_num_type  block_num() const {
                return fc::endian_reverse_u32(block_id._hash[0]);
            }

            bool operator==(const block_info_type &rhs) const {
                return block_id == rhs.block_id;
            }

            bool operator!=(const block_info_type &rhs) const {
                return !(*this == rhs);
            }

            bool empty() const {
                return block_id == block_id_type();
            }
        };

        struct pbft_message_common {
            explicit pbft_message_common(pbft_message_type t): type{t} {};

            pbft_message_type   type;
            time_point          timestamp = time_point::now();

            ~pbft_message_common() = default;
        };

        template<typename pbft_message_body>
        struct pbft_message_metadata {
            explicit pbft_message_metadata(pbft_message_body m, chain_id_type& chain_id): msg{m} {
                try {
                    sender_key = crypto::public_key(msg.sender_signature, msg.digest(chain_id), true);
                } catch (fc::exception & /*e*/) {
                    wlog("bad pbft message signature: ${m}", ("m", msg));
                }
            }

            pbft_message_body   msg;
            public_key_type     sender_key;
        };

        template<typename pbft_message_body>
        using pbft_metadata_ptr = std::shared_ptr<pbft_message_metadata<pbft_message_body>>;

        struct pbft_prepare {
            explicit pbft_prepare() = default;

            pbft_message_common common = pbft_message_common(pbft_message_type::prepare);
            pbft_view_type      view = 0;
            block_info_type     block_info;
            signature_type      sender_signature;

            bool operator<(const pbft_prepare &rhs) const {
                if (block_info.block_num() < rhs.block_info.block_num()) {
                    return true;
                } else if (block_info.block_num() == rhs.block_info.block_num()) {
                    return view < rhs.view;
                } else {
                    return false;
                }
            }

            bool empty() const {
                return !view
                && block_info.empty()
                && sender_signature == signature_type();
            }

            digest_type digest(chain_id_type chain_id) const {
                digest_type::encoder enc;
                fc::raw::pack(enc, chain_id);
                fc::raw::pack(enc, common);
                fc::raw::pack(enc, view);
                fc::raw::pack(enc, block_info);
                return enc.result();
            }
        };

        using pbft_prepare_ptr = std::shared_ptr<pbft_prepare>;

        struct pbft_commit {
            explicit pbft_commit() = default;

            pbft_message_common common = pbft_message_common(pbft_message_type::commit);
            pbft_view_type      view = 0;
            block_info_type     block_info;
            signature_type      sender_signature;

            bool operator<(const pbft_commit &rhs) const {
                if (block_info.block_num() < rhs.block_info.block_num()) {
                    return true;
                } else if (block_info.block_num() == rhs.block_info.block_num()) {
                    return view < rhs.view;
                } else {
                    return false;
                }
            }

            bool empty() const {
                return !view
                && block_info.empty()
                && sender_signature == signature_type();
            }

            digest_type digest(chain_id_type chain_id) const {
                digest_type::encoder enc;
                fc::raw::pack(enc, chain_id);
                fc::raw::pack(enc, common);
                fc::raw::pack(enc, view);
                fc::raw::pack(enc, block_info);
                return enc.result();
            }
        };

        using pbft_commit_ptr = std::shared_ptr<pbft_commit>;

        struct pbft_checkpoint {
            explicit pbft_checkpoint() = default;

            pbft_message_common common = pbft_message_common(pbft_message_type::checkpoint);
            block_info_type     block_info;
            signature_type      sender_signature;

            bool operator<(const pbft_checkpoint &rhs) const {
                return block_info.block_num() < rhs.block_info.block_num();
            }

            digest_type digest(chain_id_type chain_id) const {
                digest_type::encoder enc;
                fc::raw::pack(enc, chain_id);
                fc::raw::pack(enc, common);
                fc::raw::pack(enc, block_info);
                return enc.result();
            }
        };

        using pbft_checkpoint_ptr = std::shared_ptr<pbft_checkpoint>;

        struct pbft_stable_checkpoint {
            explicit pbft_stable_checkpoint() = default;

            block_info_type         block_info;
            vector<pbft_checkpoint> checkpoints;

            bool operator<(const pbft_stable_checkpoint &rhs) const {
                return block_info.block_num() < rhs.block_info.block_num();
            }

            bool empty() const {
                return block_info == block_info_type()
                && checkpoints.empty();
            }
        };

        struct pbft_prepared_certificate {
            explicit pbft_prepared_certificate() = default;

            block_info_type      block_info;
            set<block_id_type>   pre_prepares;
            vector<pbft_prepare> prepares;

            bool operator<(const pbft_prepared_certificate &rhs) const {
                return block_info.block_num() < rhs.block_info.block_num();
            }

            bool empty() const {
                return block_info == block_info_type()
                && prepares.empty();
            }
        };

        struct pbft_committed_certificate {
            explicit pbft_committed_certificate() = default;

            block_info_type     block_info;
            vector<pbft_commit> commits;

            bool operator<(const pbft_committed_certificate &rhs) const {
                return block_info.block_num() < rhs.block_info.block_num();
            }

            bool empty() const {
                return block_info == block_info_type()
                && commits.empty();
            }
        };

        struct pbft_view_change {
            explicit pbft_view_change() = default;

            pbft_message_common                 common = pbft_message_common(pbft_message_type::view_change);
            pbft_view_type                      current_view = 0;
            pbft_view_type                      target_view = 1;
            pbft_prepared_certificate           prepared_cert;
            vector<pbft_committed_certificate>  committed_certs;
            pbft_stable_checkpoint              stable_checkpoint;
            signature_type                      sender_signature;

            bool operator<(const pbft_view_change &rhs) const {
                return target_view < rhs.target_view;
            }

            digest_type digest(chain_id_type chain_id) const {
                digest_type::encoder enc;
                fc::raw::pack(enc, chain_id);
                fc::raw::pack(enc, common);
                fc::raw::pack(enc, current_view);
                fc::raw::pack(enc, target_view);
                fc::raw::pack(enc, prepared_cert);
                fc::raw::pack(enc, committed_certs);
                fc::raw::pack(enc, stable_checkpoint);
                return enc.result();
            }

            bool empty() const {
                return !current_view
                && target_view == 1
                && prepared_cert.empty()
                && committed_certs.empty()
                && stable_checkpoint.empty()
                && sender_signature == signature_type();
            }
        };

        using pbft_view_change_ptr = std::shared_ptr<pbft_view_change>;

        struct pbft_view_changed_certificate {
            explicit pbft_view_changed_certificate() = default;

            pbft_view_type              target_view = 0;
            vector<pbft_view_change>    view_changes;

            bool empty() const {
                return !target_view
                       && view_changes.empty();
            }
        };

        struct pbft_new_view {
            explicit pbft_new_view() = default;

            pbft_message_common                 common = pbft_message_common(pbft_message_type::new_view);
            pbft_view_type                      new_view = 0;
            pbft_prepared_certificate           prepared_cert;
            vector<pbft_committed_certificate>  committed_certs;
            pbft_stable_checkpoint              stable_checkpoint;
            pbft_view_changed_certificate       view_changed_cert;
            signature_type                      sender_signature;

            bool operator<(const pbft_new_view &rhs) const {
                return new_view < rhs.new_view;
            }

            digest_type digest(chain_id_type chain_id) const {
                digest_type::encoder enc;
                fc::raw::pack(enc, chain_id);
                fc::raw::pack(enc, common);
                fc::raw::pack(enc, new_view);
                fc::raw::pack(enc, prepared_cert);
                fc::raw::pack(enc, committed_certs);
                fc::raw::pack(enc, stable_checkpoint);
                fc::raw::pack(enc, view_changed_cert);
                return enc.result();
            }

            bool empty() const {
                return new_view == 0
                && prepared_cert.empty()
                && committed_certs.empty()
                && stable_checkpoint.empty()
                && view_changed_cert.empty()
                && sender_signature == signature_type();
            }
        };

        using pbft_new_view_ptr = std::shared_ptr<pbft_new_view>;

        struct pbft_state {
            block_id_type                                                       block_id;
            block_num_type                                                      block_num = 0;
            flat_map<std::pair<pbft_view_type, public_key_type>, pbft_prepare>  prepares;
            bool                                                                is_prepared = false;
            flat_map<std::pair<pbft_view_type, public_key_type>, pbft_commit>   commits;
            bool                                                                is_committed = false;
        };

        struct pbft_view_change_state {
            pbft_view_type                              view;
            flat_map<public_key_type, pbft_view_change> view_changes;
            bool                                        is_view_changed = false;
        };

        struct pbft_checkpoint_state {
            block_id_type                               block_id;
            block_num_type                              block_num = 0;
            flat_map<public_key_type, pbft_checkpoint>  checkpoints;
            bool                                        is_stable = false;
        };

        using pbft_state_ptr = std::shared_ptr<pbft_state>;
        using pbft_view_change_state_ptr = std::shared_ptr<pbft_view_change_state>;
        using pbft_checkpoint_state_ptr = std::shared_ptr<pbft_checkpoint_state>;

        struct by_block_id;
        struct by_num;
        struct by_prepare_and_num;
        struct by_commit_and_num;
        typedef multi_index_container<
                pbft_state_ptr,
                indexed_by<
                        hashed_unique <
                                tag<by_block_id>,
                                member<pbft_state, block_id_type, &pbft_state::block_id>,
                                std::hash<block_id_type>
                        >,
                        ordered_non_unique<
                                tag<by_num>,
                                member<pbft_state, uint32_t, &pbft_state::block_num>,
                                less<>
                        >,
                        ordered_non_unique<
                                tag<by_prepare_and_num>,
                                composite_key<
                                        pbft_state,
                                        member<pbft_state, bool, &pbft_state::is_prepared>,
                                        member<pbft_state, uint32_t, &pbft_state::block_num>
                                >,
                                composite_key_compare< greater<>, greater<> >
                        >,
                        ordered_non_unique<
                                tag<by_commit_and_num>,
                                composite_key<
                                        pbft_state,
                                        member<pbft_state, bool, &pbft_state::is_committed>,
                                        member<pbft_state, uint32_t, &pbft_state::block_num>
                                >,
                                composite_key_compare< greater<>, greater<> >
                        >
                >
        > pbft_state_multi_index_type;

        struct by_view;
        struct by_count_and_view;
        typedef multi_index_container<
                pbft_view_change_state_ptr,
                indexed_by<
                        ordered_unique<
                                tag<by_view>,
                                member<pbft_view_change_state, pbft_view_type, &pbft_view_change_state::view>,
                                greater<>
                        >,
                        ordered_non_unique<
                                tag<by_count_and_view>,
                                composite_key<
                                        pbft_view_change_state,
                                        member<pbft_view_change_state, bool, &pbft_view_change_state::is_view_changed>,
                                        member<pbft_view_change_state, pbft_view_type, &pbft_view_change_state::view>
                                >,
                                composite_key_compare<greater<>, greater<>>
                        >
                >
        > pbft_view_state_multi_index_type;

        struct by_block_id;
        struct by_num;
        typedef multi_index_container<
                pbft_checkpoint_state_ptr,
                indexed_by<
                        hashed_unique<
                                tag<by_block_id>,
                                member<pbft_checkpoint_state, block_id_type, &pbft_checkpoint_state::block_id>,
                                std::hash<block_id_type>
                        >,
                        ordered_non_unique<
                                tag<by_num>,
                                member<pbft_checkpoint_state, uint32_t, &pbft_checkpoint_state::block_num>,
                                less<>
                        >
                >
        > pbft_checkpoint_state_multi_index_type;

        class pbft_database {
        public:
            explicit pbft_database(controller &ctrl);

            ~pbft_database();

            void close();

            void add_pbft_prepare(pbft_prepare &p, const public_key_type &pk);
            void add_pbft_commit(pbft_commit &c, const public_key_type &pk);
            void add_pbft_view_change(pbft_view_change &vc, const public_key_type &pk);
            void add_pbft_checkpoint(pbft_checkpoint &cp, const public_key_type &pk);

            pbft_prepare send_and_add_pbft_prepare(const pbft_prepare &cached_prepare = pbft_prepare(), pbft_view_type current_view = 0);
            pbft_commit send_and_add_pbft_commit(const pbft_commit &cached_commit = pbft_commit(), pbft_view_type current_view = 0);
            pbft_view_change send_and_add_pbft_view_change(
                    const pbft_view_change &cached_view_change = pbft_view_change(),
                    const pbft_prepared_certificate &ppc = pbft_prepared_certificate(),
                    const vector<pbft_committed_certificate> &pcc = vector<pbft_committed_certificate>{},
                    pbft_view_type current_view = 0,
                    pbft_view_type target_view = 1);
            pbft_new_view send_pbft_new_view(
                    const pbft_view_changed_certificate &vcc = pbft_view_changed_certificate(),
                    pbft_view_type current_view = 1);
            vector<pbft_checkpoint> generate_and_add_pbft_checkpoint();
            void send_pbft_checkpoint();

            bool should_prepared();
            bool should_committed();
            pbft_view_type should_view_change();
            bool should_new_view(pbft_view_type target_view);

            //new view
            bool has_new_primary(const public_key_type &pk);
            pbft_view_type get_proposed_new_view_num();
            pbft_view_type get_committed_view();
            public_key_type get_new_view_primary_key(pbft_view_type target_view);

            void mark_as_prepared(const block_id_type &bid);
            void mark_as_committed(const block_id_type &bid);
            void commit_local();
            void checkpoint_local();

            //view change
            pbft_prepared_certificate generate_prepared_certificate();
            vector<pbft_committed_certificate> generate_committed_certificate();
            pbft_view_changed_certificate generate_view_changed_certificate(pbft_view_type target_view);
            bool should_stop_view_change(const pbft_view_change &vc);

            //validations
            bool is_valid_prepare(const pbft_prepare &p, const public_key_type &pk);
            bool is_valid_commit(const pbft_commit &c, const public_key_type &pk);
            bool is_valid_checkpoint(const pbft_checkpoint &cp, const public_key_type &pk);
            bool is_valid_view_change(const pbft_view_change &vc, const public_key_type &pk);
            void validate_new_view(const pbft_new_view &nv, const public_key_type &pk);
            bool is_valid_stable_checkpoint(const pbft_stable_checkpoint &scp, bool add_to_pbft_db = false);
            bool should_send_pbft_msg();
            bool should_recv_pbft_msg(const public_key_type &pub_key);

            bool pending_pbft_lib();
            chain_id_type& get_chain_id() {return chain_id;}
            pbft_stable_checkpoint get_stable_checkpoint_by_id(const block_id_type &block_id, bool incl_blk_extn = true);
            pbft_stable_checkpoint fetch_stable_checkpoint_from_blk_extn(const signed_block_ptr &b);
            block_info_type cal_pending_stable_checkpoint() const;

            void cleanup_on_new_view();
            void update_fork_schedules();

            //api related
            pbft_state_ptr get_pbft_state_by_id(const block_id_type &id) const;
            vector<pbft_checkpoint_state> get_checkpoints_by_num(const block_num_type &num) const;
            pbft_view_change_state_ptr get_view_changes_by_target_view(const pbft_view_type &tv) const;
            vector<block_num_type> get_pbft_watermarks() const;
            flat_map<public_key_type, uint32_t> get_pbft_fork_schedules() const;


            signal<void(const pbft_prepare_ptr &)> pbft_outgoing_prepare;
            signal<void(const pbft_commit_ptr &)> pbft_outgoing_commit;
            signal<void(const pbft_view_change_ptr &)> pbft_outgoing_view_change;
            signal<void(const pbft_new_view_ptr &)> pbft_outgoing_new_view;
            signal<void(const pbft_checkpoint_ptr &)> pbft_outgoing_checkpoint;

        private:
            controller                                  &ctrl;
            pbft_state_multi_index_type                 pbft_state_index;
            pbft_view_state_multi_index_type            view_state_index;
            pbft_checkpoint_state_multi_index_type      checkpoint_index;
            fc::path                                    pbft_db_dir;
            fc::path                                    checkpoints_dir;
            vector<block_num_type>                      prepare_watermarks;
            flat_map<public_key_type, block_num_type>   fork_schedules;
            chain_id_type                               chain_id = ctrl.get_chain_id();


            bool is_less_than_high_watermark(const block_num_type &bnum);
            bool is_valid_prepared_certificate(const pbft_prepared_certificate &certificate, bool add_to_pbft_db = false);
            bool is_valid_committed_certificate(const pbft_committed_certificate &certificate, bool add_to_pbft_db = false);
            bool is_valid_longest_fork(const block_info_type &bi, vector<block_info_type> block_infos, unsigned long threshold, unsigned long non_fork_bp_count);

            producer_schedule_type lscb_active_producers() const;
            vector<block_num_type>& get_updated_watermarks();
            flat_map<public_key_type, uint32_t>& get_updated_fork_schedules();
            block_num_type get_current_pbft_watermark();

            vector<vector<block_info_type>> fetch_fork_from(vector<block_info_type> &block_infos);
            vector<block_info_type> fetch_first_fork_from(vector<block_info_type> &bi);

            template<typename Signal, typename Arg>
            void emit(const Signal &s, Arg &&a);

            void set(const pbft_state_ptr& s);
            void set(const pbft_checkpoint_state_ptr& s);
            void prune(const pbft_state_ptr &h);
            void prune(const pbft_checkpoint_state_ptr &h);
        };
    }
} /// namespace eosio::chain

FC_REFLECT(eosio::chain::block_info_type, (block_id))
FC_REFLECT_ENUM(eosio::chain::pbft_message_type, (prepare)(commit)(checkpoint)(view_change)(new_view))

FC_REFLECT(eosio::chain::pbft_message_common, (type)(timestamp))

FC_REFLECT_TEMPLATE((typename pbft_message_body), eosio::chain::pbft_message_metadata<pbft_message_body>, (msg)(sender_key))

FC_REFLECT(eosio::chain::pbft_prepare, (common)(view)(block_info)(sender_signature))
FC_REFLECT(eosio::chain::pbft_commit, (common)(view)(block_info)(sender_signature))
FC_REFLECT(eosio::chain::pbft_checkpoint,(common)(block_info)(sender_signature))
FC_REFLECT(eosio::chain::pbft_view_change, (common)(current_view)(target_view)(prepared_cert)(committed_certs)(stable_checkpoint)(sender_signature))
FC_REFLECT(eosio::chain::pbft_new_view, (common)(new_view)(prepared_cert)(committed_certs)(stable_checkpoint)(view_changed_cert)(sender_signature))


FC_REFLECT(eosio::chain::pbft_prepared_certificate, (block_info)(pre_prepares)(prepares))
FC_REFLECT(eosio::chain::pbft_committed_certificate,(block_info)(commits))
FC_REFLECT(eosio::chain::pbft_view_changed_certificate, (target_view)(view_changes))
FC_REFLECT(eosio::chain::pbft_stable_checkpoint, (block_info)(checkpoints))

FC_REFLECT(eosio::chain::pbft_state, (block_id)(block_num)(prepares)(is_prepared)(commits)(is_committed))
FC_REFLECT(eosio::chain::pbft_view_change_state, (view)(view_changes)(is_view_changed))
FC_REFLECT(eosio::chain::pbft_checkpoint_state, (block_id)(block_num)(checkpoints)(is_stable))