/**
 *  @file
 *  @copyright defined in eos/LICENSE
 */
#include <eosio/txn_test_gen_plugin/txn_test_gen_plugin.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/net_plugin/net_plugin.hpp>
#include <eosio/chain/wast_to_wasm.hpp>
#include <eosio/chain/thread_utils.hpp>

#include <fc/variant.hpp>
#include <fc/io/json.hpp>
#include <fc/exception/exception.hpp>
#include <fc/reflect/variant.hpp>

#include <boost/asio/high_resolution_timer.hpp>
#include <boost/algorithm/clamp.hpp>

#include <Inline/BasicTypes.h>

namespace eosio { namespace detail {
struct txn_test_gen_empty {};
}}

FC_REFLECT(eosio::detail::txn_test_gen_empty, );

namespace eosio {

static appbase::abstract_plugin& _txn_test_gen_plugin = app().register_plugin<txn_test_gen_plugin>();

using namespace eosio::chain;

#define CALL(api_name, api_handle, call_name, INVOKE, http_response_code) \
{std::string("/v1/" #api_name "/" #call_name), \
   [this](string, string body, url_response_callback cb) mutable { \
          try { \
             if (body.empty()) body = "{}"; \
             INVOKE \
             cb(http_response_code, fc::variant(result)); \
          } catch (...) { \
             http_plugin::handle_exception(#api_name, #call_name, body, cb); \
          } \
       }}

#define INVOKE_V_R_R_R(api_handle, call_name, in_param0, in_param1, in_param2) \
     const auto& vs = fc::json::json::from_string(body).as<fc::variants>(); \
     api_handle->call_name(vs.at(0).as<in_param0>(), vs.at(1).as<in_param1>(), vs.at(2).as<in_param2>()); \
     eosio::detail::txn_test_gen_empty result;

#define INVOKE_V_R_R(api_handle, call_name, in_param0, in_param1) \
     const auto& vs = fc::json::json::from_string(body).as<fc::variants>(); \
     api_handle->call_name(vs.at(0).as<in_param0>(), vs.at(1).as<in_param1>()); \
     eosio::detail::txn_test_gen_empty result;

#define INVOKE_V_V(api_handle, call_name) \
     api_handle->call_name(); \
     eosio::detail::txn_test_gen_empty result;

#define CALL_ASYNC(api_name, api_handle, call_name, INVOKE, http_response_code) \
{std::string("/v1/" #api_name "/" #call_name), \
   [this](string, string body, url_response_callback cb) mutable { \
      if (body.empty()) body = "{}"; \
      auto result_handler = [cb, body](const fc::exception_ptr& e) {\
         if (e) {\
            try {\
               e->dynamic_rethrow_exception();\
            } catch (...) {\
               http_plugin::handle_exception(#api_name, #call_name, body, cb);\
            }\
         } else {\
            cb(http_response_code, fc::variant(eosio::detail::txn_test_gen_empty())); \
         }\
      };\
      INVOKE \
   }\
}

#define INVOKE_ASYNC_R_R(api_handle, call_name, in_param0, in_param1, in_param2) \
   const auto& vs = fc::json::json::from_string(body).as<fc::variants>(); \
   api_handle->call_name(vs.at(0).as<in_param0>(), vs.at(1).as<in_param1>(), vs.at(2).as<in_param2>(), result_handler);

struct txn_test_gen_plugin_impl {

  uint64_t _total_us = 0;
  uint64_t _txcount = 0;

  int _remain = 0;

  std::string cached_salt;
  uint64_t cached_period;
  uint64_t cached_batch_size;

  void push_next_transaction(const std::shared_ptr<std::vector<signed_transaction>>& trxs, size_t index, const std::function<void(const fc::exception_ptr&)>& next ) {
      chain_plugin& cp = app().get_plugin<chain_plugin>();

      const int overlap = 20;
      int end = std::min(index + overlap, trxs->size());
      _remain = end - index;
      for (int i = index; i < end; ++i) {
          cp.accept_transaction( packed_transaction(trxs->at(i)), [=](const fc::static_variant<fc::exception_ptr, transaction_trace_ptr>& result){
            if (result.contains<fc::exception_ptr>()) {
                next(result.get<fc::exception_ptr>());
            } else {
                if (result.contains<transaction_trace_ptr>() && result.get<transaction_trace_ptr>()->receipt) {
                    _total_us += result.get<transaction_trace_ptr>()->receipt->cpu_usage_us;
                    ++_txcount;
                }
                --_remain;
                if (_remain == 0 ) {
                    if (end < trxs->size()) {
                        push_next_transaction(trxs, index + overlap, next);
                    } else {
                        next(nullptr);
                    }
                }
            }
          });
      }
  }

  void push_transactions( std::vector<signed_transaction>&& trxs, const std::function<void(fc::exception_ptr)>& next ) {
      auto trxs_copy = std::make_shared<std::decay_t<decltype(trxs)>>(std::move(trxs));
      push_next_transaction(trxs_copy, 0, next);
  }

  void create_test_accounts(const std::string& init_name, const std::string& init_priv_key,
          const std::string& core_symbol,
          const std::function<void(const fc::exception_ptr&)>& next) {
      std::vector<signed_transaction> trxs;
      trxs.reserve(2);

      try {
          name newaccountA("aaaaaaaaaaaa");
          name newaccountB("bbbbbbbbbbbb");
          name newaccountC("cccccccccccc");
          name creator(init_name);

          controller& cc = app().get_plugin<chain_plugin>().chain();
          auto chainid = app().get_plugin<chain_plugin>().get_chain_id();
          auto abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

          const auto& eosio_token_acnt = cc.get_account(N(eosio.token));
          auto eosio_token_abi = eosio_token_acnt.get_abi();
          abi_serializer eosio_token_serializer(eosio_token_abi, abi_serializer_max_time);

          fc::crypto::private_key txn_test_receiver_A_priv_key = fc::crypto::private_key::regenerate(fc::sha256(std::string(64, 'a')));
          fc::crypto::private_key txn_test_receiver_B_priv_key = fc::crypto::private_key::regenerate(fc::sha256(std::string(64, 'b')));
          fc::crypto::private_key txn_test_receiver_C_priv_key = fc::crypto::private_key::regenerate(fc::sha256(std::string(64, 'c')));
          fc::crypto::public_key  txn_text_receiver_A_pub_key = txn_test_receiver_A_priv_key.get_public_key();
          fc::crypto::public_key  txn_text_receiver_B_pub_key = txn_test_receiver_B_priv_key.get_public_key();
          fc::crypto::public_key  txn_text_receiver_C_pub_key = txn_test_receiver_C_priv_key.get_public_key();
          fc::crypto::private_key creator_priv_key = fc::crypto::private_key(init_priv_key);

          eosio::chain::asset net{1000000, symbol(4,core_symbol.c_str())};
          eosio::chain::asset cpu{10000000000, symbol(4,core_symbol.c_str())};
          eosio::chain::asset ram{1000000, symbol(4,core_symbol.c_str())};

          //create some test accounts
          {
              signed_transaction trx;

              //create "A" account
              {
                  auto owner_auth   = eosio::chain::authority{1, {{txn_text_receiver_A_pub_key, 1}}, {}};
                  auto active_auth  = eosio::chain::authority{1, {{txn_text_receiver_A_pub_key, 1}}, {}};

                  trx.actions.emplace_back(vector<chain::permission_level>{{creator,name("active")}}, newaccount{creator, newaccountA, owner_auth, active_auth});

                  //delegate cpu net and buyram
                  auto act_delegatebw = create_action_delegatebw(creator, newaccountA,net,cpu,abi_serializer_max_time);
                  auto act_buyram = create_action_buyram(creator, newaccountA, ram, abi_serializer_max_time);

                  trx.actions.emplace_back(act_delegatebw);
                  trx.actions.emplace_back(act_buyram);

              }
              //create "B" account
              {
                  auto owner_auth   = eosio::chain::authority{1, {{txn_text_receiver_B_pub_key, 1}}, {}};
                  auto active_auth  = eosio::chain::authority{1, {{txn_text_receiver_B_pub_key, 1}}, {}};

                  trx.actions.emplace_back(vector<chain::permission_level>{{creator,name("active")}}, newaccount{creator, newaccountB, owner_auth, active_auth});

                  //delegate cpu net and buyram
                  auto act_delegatebw = create_action_delegatebw(creator, newaccountB,net,cpu,abi_serializer_max_time);
                  auto act_buyram = create_action_buyram(creator, newaccountB, ram, abi_serializer_max_time);

                  trx.actions.emplace_back(act_delegatebw);
                  trx.actions.emplace_back(act_buyram);
              }
              //create "cccccccccccc" account
              {
                  auto owner_auth   = eosio::chain::authority{1, {{txn_text_receiver_C_pub_key, 1}}, {}};
                  auto active_auth  = eosio::chain::authority{1, {{txn_text_receiver_C_pub_key, 1}}, {}};

                  trx.actions.emplace_back(vector<chain::permission_level>{{creator,name("active")}}, newaccount{creator, newaccountC, owner_auth, active_auth});

                  //delegate cpu net and buyram
                  auto act_delegatebw = create_action_delegatebw(creator, newaccountC,net,cpu,abi_serializer_max_time);
                  auto act_buyram = create_action_buyram(creator, newaccountC, ram, abi_serializer_max_time);

                  trx.actions.emplace_back(act_delegatebw);
                  trx.actions.emplace_back(act_buyram);
              }

              trx.expiration = cc.head_block_time() + fc::seconds(30);
              trx.set_reference_block(cc.head_block_id());
              trx.sign(creator_priv_key, chainid);
              trxs.emplace_back(std::move(trx));
          }

          //set cccccccccccc contract to eosio.token & initialize it
          {
              signed_transaction trx;

//            vector<uint8_t> wasm = contracts::eosio_token_wasm();
              string wasm = app().get_plugin<chain_plugin>().get_read_only_api().get_code({N(eosio.token), true}).wasm;

              setcode handler;
              handler.account = newaccountC;
              handler.code.assign(wasm.begin(), wasm.end());

              trx.actions.emplace_back( vector<chain::permission_level>{{newaccountC,name("active")}}, handler);

              {
                  setabi handler;
                  handler.account = newaccountC;
				  handler.abi = fc::raw::pack(eosio_token_abi);
                  trx.actions.emplace_back( vector<chain::permission_level>{{newaccountC,name("active")}}, handler);
              }

              {
                  action act;
                  act.account = N(cccccccccccc);
                  act.name = N(create);
                  act.authorization = vector<permission_level>{{newaccountC,config::active_name}};
                  act.data = eosio_token_serializer.variant_to_binary("create", fc::json::from_string("{\"issuer\":\"cccccccccccc\",\"maximum_supply\":\"1000000000.0000 CUR\"}}"), abi_serializer_max_time);
                  trx.actions.push_back(act);
              }
              {
                  action act;
                  act.account = N(cccccccccccc);
                  act.name = N(issue);
                  act.authorization = vector<permission_level>{{newaccountC,config::active_name}};
                  act.data = eosio_token_serializer.variant_to_binary("issue", fc::json::from_string("{\"to\":\"cccccccccccc\",\"quantity\":\"1000000000.0000 CUR\",\"memo\":\"\"}"), abi_serializer_max_time);
                  trx.actions.push_back(act);
              }
              {
                  action act;
                  act.account = N(cccccccccccc);
                  act.name = N(transfer);
                  act.authorization = vector<permission_level>{{newaccountC,config::active_name}};
                  act.data = eosio_token_serializer.variant_to_binary("transfer", fc::json::from_string("{\"from\":\"cccccccccccc\",\"to\":\"aaaaaaaaaaaa\",\"quantity\":\"500000000.0000 CUR\",\"memo\":\"\"}"), abi_serializer_max_time);
                  trx.actions.push_back(act);
              }
              {
                  action act;
                  act.account = N(cccccccccccc);
                  act.name = N(transfer);
                  act.authorization = vector<permission_level>{{newaccountC,config::active_name}};
                  act.data = eosio_token_serializer.variant_to_binary("transfer", fc::json::from_string("{\"from\":\"cccccccccccc\",\"to\":\"bbbbbbbbbbbb\",\"quantity\":\"500000000.0000 CUR\",\"memo\":\"\"}"), abi_serializer_max_time);
                  trx.actions.push_back(act);
              }

              trx.expiration = cc.head_block_time() + fc::seconds(30);
              trx.set_reference_block(cc.head_block_id());
              trx.max_net_usage_words = 5000;
              trx.sign(txn_test_receiver_C_priv_key, chainid);
              trxs.emplace_back(std::move(trx));
          }
      } catch (const fc::exception& e) {
          next(e.dynamic_copy_exception());
          return;
      }

      push_transactions(std::move(trxs), next);
  }

  eosio::chain::action create_action_delegatebw(const name &from, const name &to, const asset &net, const asset &cpu, const fc::microseconds &abi_serializer_max_time){
      fc::variant variant_delegate = fc::mutable_variant_object()
              ("from", from.to_string())
              ("receiver", to.to_string())
              ("stake_net_quantity", net.to_string())
              ("stake_cpu_quantity", cpu.to_string())
              ("transfer", true);
	  controller& cc = app().get_plugin<chain_plugin>().chain();
	  const auto& eosio_acnt = cc.get_account(N(eosio));
	  auto eosio_abi = eosio_acnt.get_abi();
	  abi_serializer eosio_system_serializer(eosio_abi, abi_serializer_max_time);

      auto payload_delegate = eosio_system_serializer.variant_to_binary( "delegatebw", variant_delegate, abi_serializer_max_time);
      eosio::chain::action act_delegate{vector<chain::permission_level>{{from,name("active")}},
              config::system_account_name, N(delegatebw), payload_delegate};

      return act_delegate;
  }

  eosio::chain::action create_action_buyram(const name &from, const name &to, const asset &quant, const fc::microseconds &abi_serializer_max_time){
      fc::variant variant_buyram = fc::mutable_variant_object()
              ("payer", from.to_string())
              ("receiver", to.to_string())
              ("quant", quant.to_string());
	  controller& cc = app().get_plugin<chain_plugin>().chain();
	  const auto& eosio_acnt = cc.get_account(N(eosio));
	  auto eosio_abi = eosio_acnt.get_abi();
	  abi_serializer eosio_system_serializer(eosio_abi, abi_serializer_max_time);

	  auto payload_buyram = eosio_system_serializer.variant_to_binary( "buyram", variant_buyram, abi_serializer_max_time);
      eosio::chain::action act_buyram{vector<chain::permission_level>{{from,name("active")}},
              config::system_account_name, N(buyram), payload_buyram};

      return act_buyram;
  }

  void start_generation(const std::string& salt, const uint64_t& period, const uint64_t& batch_size) {
      if(running)
          throw fc::exception(fc::invalid_operation_exception_code);
      if(period < 1 || period > 2500)
          throw fc::exception(fc::invalid_operation_exception_code);
      if(batch_size < 1 || batch_size > 250)
          throw fc::exception(fc::invalid_operation_exception_code);
      if(batch_size & 1)
          throw fc::exception(fc::invalid_operation_exception_code);

      running = true;
      cached_salt = salt;
      cached_period = period;
      cached_batch_size = batch_size;

      controller& cc = app().get_plugin<chain_plugin>().chain();
      auto abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();
	  const auto& eosio_token_acnt = cc.get_account(N(eosio.token));
	  auto eosio_token_abi = eosio_token_acnt.get_abi();
	  abi_serializer eosio_token_serializer(eosio_token_abi, abi_serializer_max_time);
      //create the actions here
      act_a_to_b.account = N(cccccccccccc);
      act_a_to_b.name = N(transfer);
      act_a_to_b.authorization = vector<permission_level>{{name("aaaaaaaaaaaa"),config::active_name}};
      act_a_to_b.data = eosio_token_serializer.variant_to_binary("transfer",
              fc::json::from_string(fc::format_string("{\"from\":\"aaaaaaaaaaaa\",\"to\":\"bbbbbbbbbbbb\",\"quantity\":\"1.0000 CUR\",\"memo\":\"${l}\"}",
                      fc::mutable_variant_object()("l", salt))),
              abi_serializer_max_time);

      act_b_to_a.account = N(cccccccccccc);
      act_b_to_a.name = N(transfer);
      act_b_to_a.authorization = vector<permission_level>{{name("bbbbbbbbbbbb"),config::active_name}};
      act_b_to_a.data = eosio_token_serializer.variant_to_binary("transfer",
              fc::json::from_string(fc::format_string("{\"from\":\"bbbbbbbbbbbb\",\"to\":\"aaaaaaaaaaaa\",\"quantity\":\"1.0000 CUR\",\"memo\":\"${l}\"}",
                      fc::mutable_variant_object()("l", salt))),
              abi_serializer_max_time);

      timer_timeout = period;
      batch = batch_size/2;

      ilog("Started transaction test plugin; performing ${p} transactions every ${m}ms", ("p", batch_size)("m", period));
      ilog("wait 3 seconds to spin up");
      arm_timer(boost::asio::high_resolution_timer::clock_type::now() + std::chrono::milliseconds(3000) );
  }

  void arm_timer(boost::asio::high_resolution_timer::time_point s) {
      timer.expires_at(s + std::chrono::milliseconds(timer_timeout));
      timer.async_wait([this](const boost::system::error_code& ec) {
        if(!running || ec)
            return;

        send_transaction([this](const fc::exception_ptr& e){
          if (e) {
              elog("pushing transaction failed: ${e}", ("e", e->to_detail_string()));
              stop_generation();
              auto peers_conn = app().get_plugin<net_plugin>().connections();
              for(const auto c : peers_conn){
                  app().get_plugin<net_plugin>().disconnect(c.peer);
              }
              for(const auto c : peers_conn){
                  app().get_plugin<net_plugin>().connect(c.peer);
              }
              start_generation(cached_salt,cached_period,cached_batch_size);
          } else {
              arm_timer(timer.expires_at());
          }
        });
      });
  }

  void send_transaction(std::function<void(const fc::exception_ptr&)> next) {
      std::vector<signed_transaction> trxs;
      trxs.reserve(2*batch);

      try {
          controller& cc = app().get_plugin<chain_plugin>().chain();
          auto chainid = app().get_plugin<chain_plugin>().get_chain_id();

          static fc::crypto::private_key a_priv_key = fc::crypto::private_key::regenerate(fc::sha256(std::string(64, 'a')));
          static fc::crypto::private_key b_priv_key = fc::crypto::private_key::regenerate(fc::sha256(std::string(64, 'b')));

          static uint64_t nonce = static_cast<uint64_t>(fc::time_point::now().sec_since_epoch()) << 32;

          uint32_t reference_block_num = cc.last_irreversible_block_num();
          if (txn_reference_block_lag >= 0) {
              reference_block_num = cc.head_block_num();
              if (reference_block_num <= (uint32_t)txn_reference_block_lag) {
                  reference_block_num = 0;
              } else {
                  reference_block_num -= (uint32_t)txn_reference_block_lag;
              }
          }

          block_id_type reference_block_id = cc.get_block_id_for_num(reference_block_num);

          for(unsigned int i = 0; i < batch; ++i) {
              {
                  signed_transaction trx;
                  trx.actions.push_back(act_a_to_b);
                  trx.context_free_actions.emplace_back(action({}, config::null_account_name, name("nonce"), fc::raw::pack(nonce++)));
                  trx.set_reference_block(reference_block_id);
                  trx.expiration = cc.head_block_time() + fc::seconds(30);
                  trx.max_net_usage_words = 100;
                  trx.sign(a_priv_key, chainid);
                  trxs.emplace_back(std::move(trx));
              }

              {
                  signed_transaction trx;
                  trx.actions.push_back(act_b_to_a);
                  trx.context_free_actions.emplace_back(action({}, config::null_account_name, name("nonce"), fc::raw::pack(nonce++)));
                  trx.set_reference_block(reference_block_id);
                  trx.expiration = cc.head_block_time() + fc::seconds(30);
                  trx.max_net_usage_words = 100;
                  trx.sign(b_priv_key, chainid);
                  trxs.emplace_back(std::move(trx));
              }
          }
      } catch ( const fc::exception& e ) {
          next(e.dynamic_copy_exception());
      }

      push_transactions(std::move(trxs), next);
  }

  void stop_generation() {
      if(!running)
          throw fc::exception(fc::invalid_operation_exception_code);
      timer.cancel();
      running = false;
      ilog("Stopping transaction generation test");

      if (_txcount) {
          ilog("${d} transactions executed, ${t}us / transaction", ("d", _txcount)("t", _total_us / (double)_txcount));
          _txcount = _total_us = 0;
      }
  }

  boost::asio::high_resolution_timer timer{app().get_io_service()};
  bool running{false};

  unsigned timer_timeout;
  unsigned batch;

  action act_a_to_b;
  action act_b_to_a;

  int32_t txn_reference_block_lag;
};

txn_test_gen_plugin::txn_test_gen_plugin() {}
txn_test_gen_plugin::~txn_test_gen_plugin() {}

void txn_test_gen_plugin::set_program_options(options_description&, options_description& cfg) {
    cfg.add_options()
            ("txn-reference-block-lag", bpo::value<int32_t>()->default_value(0), "Lag in number of blocks from the head block when selecting the reference block for transactions (-1 means Last Irreversible Block)")
            ;
}

void txn_test_gen_plugin::plugin_initialize(const variables_map& options) {
    try {
        my.reset( new txn_test_gen_plugin_impl );
        my->txn_reference_block_lag = options.at( "txn-reference-block-lag" ).as<int32_t>();
    } FC_LOG_AND_RETHROW()
}

void txn_test_gen_plugin::plugin_startup() {
    app().get_plugin<http_plugin>().add_api({
            CALL_ASYNC(txn_test_gen, my, create_test_accounts, INVOKE_ASYNC_R_R(my, create_test_accounts, std::string, std::string, std::string), 200),
            CALL(txn_test_gen, my, stop_generation, INVOKE_V_V(my, stop_generation), 200),
            CALL(txn_test_gen, my, start_generation, INVOKE_V_R_R_R(my, start_generation, std::string, uint64_t, uint64_t), 200)
    });
}

void txn_test_gen_plugin::plugin_shutdown() {
    try {
        my->stop_generation();
    }
    catch(fc::exception e) {
    }
}

}
