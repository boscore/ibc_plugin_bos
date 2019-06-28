/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once
#include <appbase/application.hpp>
#include <eosio/chain/pbft.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/net_plugin/net_plugin.hpp>

namespace eosio {

using namespace appbase;

class pbft_plugin : public appbase::plugin<pbft_plugin> {
public:
   pbft_plugin();
   virtual ~pbft_plugin();

   APPBASE_PLUGIN_REQUIRES()
   virtual void set_program_options(options_description&, options_description& cfg) override;
 
   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();


   pbft_state get_pbft_record( const block_id_type& bid )const;
   vector<pbft_checkpoint_state> get_pbft_checkpoints_record(const block_num_type &bnum)const;
   pbft_view_change_state get_view_change_record(const pbft_view_type& view)const;
   vector<block_num_type> get_watermarks()const;
   flat_map<public_key_type, uint32_t> get_fork_schedules()const;
   const char* get_pbft_status()const;
   block_id_type get_pbft_prepared_id()const;
   block_id_type get_pbft_my_prepare_id()const;

   void set_pbft_current_view(const pbft_view_type &view);


private:
   std::unique_ptr<class pbft_plugin_impl> my;
};

}
