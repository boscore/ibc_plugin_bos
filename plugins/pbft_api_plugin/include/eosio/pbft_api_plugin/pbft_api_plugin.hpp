/**
 *  @file
 *  @copyright defined in eos/LICENSE
 */
#pragma once

#include <eosio/pbft_plugin/pbft_plugin.hpp>
#include <eosio/http_plugin/http_plugin.hpp>

#include <appbase/application.hpp>

namespace eosio {

using namespace appbase;

class pbft_api_plugin : public plugin<pbft_api_plugin> {
   public:
      APPBASE_PLUGIN_REQUIRES( (pbft_plugin)(http_plugin))

      pbft_api_plugin() = default;
      pbft_api_plugin(const pbft_api_plugin&) = delete;
      pbft_api_plugin(pbft_api_plugin&&) = delete;
      pbft_api_plugin& operator=(const pbft_api_plugin&) = delete;
      pbft_api_plugin& operator=(pbft_api_plugin&&) = delete;
      virtual ~pbft_api_plugin() override = default;

      virtual void set_program_options(options_description& cli, options_description& cfg) override {}
      void plugin_initialize(const variables_map& vm);
      void plugin_startup();
      void plugin_shutdown() {}

   private:
};

}
