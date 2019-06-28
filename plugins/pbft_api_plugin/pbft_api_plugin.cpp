/**
 *  @file
 *  @copyright defined in eos/LICENSE
 */
#include <eosio/pbft_api_plugin/pbft_api_plugin.hpp>
#include <eosio/chain/exceptions.hpp>

#include <fc/variant.hpp>
#include <fc/io/json.hpp>

#include <chrono>

namespace eosio { namespace detail {
  struct pbft_api_plugin_response {
     std::string result;
  };
}}

FC_REFLECT(eosio::detail::pbft_api_plugin_response, (result));

namespace eosio {

static appbase::abstract_plugin& _pbft_api_plugin = app().register_plugin<pbft_api_plugin>();

using namespace eosio;

#define CALL(api_name, api_handle, call_name, INVOKE, http_response_code) \
{std::string("/v1/" #api_name "/" #call_name), \
   [&api_handle](string, string body, url_response_callback cb) mutable { \
          try { \
             if (body.empty()) body = "{}"; \
             INVOKE \
             cb(http_response_code, fc::json::to_string(result)); \
          } catch (...) { \
             http_plugin::handle_exception(#api_name, #call_name, body, cb); \
          } \
       }}
#define INVOKE_R(api_handle, call_name) \
     auto result = api_handle.call_name();

#define INVOKE_R_P(api_handle, call_name, in_param) \
     auto result = api_handle.call_name(fc::json::from_string(body).as<in_param>());

#define INVOKE_W_P(api_handle, call_name, in_param) \
     api_handle.call_name(fc::json::from_string(body).as<in_param>()); \
     eosio::detail::pbft_api_plugin_response result{"ok"};

void pbft_api_plugin::plugin_startup() {
   ilog("starting pbft_api_plugin");
   // lifetime of plugin is lifetime of application
   auto& pbft = app().get_plugin<pbft_plugin>();

   app().get_plugin<http_plugin>().add_api({
       CALL(pbft, pbft, get_watermarks, INVOKE_R(pbft, get_watermarks), 200),
       CALL(pbft, pbft, get_fork_schedules, INVOKE_R(pbft, get_fork_schedules), 200),
       CALL(pbft, pbft, get_pbft_record, INVOKE_R_P(pbft, get_pbft_record, block_id_type), 200),
       CALL(pbft, pbft, get_pbft_checkpoints_record, INVOKE_R_P(pbft, get_pbft_checkpoints_record, block_num_type), 200),
       CALL(pbft, pbft, get_view_change_record, INVOKE_R_P(pbft, get_view_change_record, pbft_view_type), 200),
       CALL(pbft, pbft, get_pbft_status, INVOKE_R(pbft, get_pbft_status), 200),
       CALL(pbft, pbft, get_pbft_prepared_id, INVOKE_R(pbft, get_pbft_prepared_id), 200),
       CALL(pbft, pbft, get_pbft_my_prepare_id, INVOKE_R(pbft, get_pbft_my_prepare_id), 200),
       CALL(pbft, pbft, set_pbft_current_view, INVOKE_W_P(pbft, set_pbft_current_view, pbft_view_type), 201),
   });
}

void pbft_api_plugin::plugin_initialize(const variables_map& options) {
   try {
      const auto& _http_plugin = app().get_plugin<http_plugin>();
      if( !_http_plugin.is_on_loopback()) {
         wlog( "\n"
               "**********SECURITY WARNING**********\n"
               "*                                  *\n"
               "* --          PBFT API          -- *\n"
               "* - EXPOSED to the LOCAL NETWORK - *\n"
               "* - USE ONLY ON SECURE NETWORKS! - *\n"
               "*                                  *\n"
               "************************************\n" );

      }
   } FC_LOG_AND_RETHROW()
}

#undef INVOKE_R
#undef INVOKE_R_P
#undef INVOKE_W_P


#undef CALL

}
