#pragma once

#include <eosio/chain/types.hpp>
#include <eosio/chain/snapshot.hpp>
#include <eosio/chain/whitelisted_intrinsics.hpp>
#include <chainbase/chainbase.hpp>
#include "multi_index_includes.hpp"

namespace eosio { namespace chain {

/**
 * @class protocol_state_object
 * @brief Maintains global state information about consensus protocol rules
 * @ingroup object
 * @ingroup implementation
 */
class protocol_state_object : public chainbase::object<protocol_state_object_type, protocol_state_object>
{
  OBJECT_CTOR(protocol_state_object, (whitelisted_intrinsics))

  id_type                                    id;
  whitelisted_intrinsics_type                whitelisted_intrinsics;
};

using protocol_state_multi_index = chainbase::shared_multi_index_container<
	protocol_state_object,
	indexed_by<
		ordered_unique<tag<by_id>,
					   BOOST_MULTI_INDEX_MEMBER(protocol_state_object, protocol_state_object::id_type, id)
		>
	>
>;

}}

CHAINBASE_SET_INDEX_TYPE(eosio::chain::protocol_state_object, eosio::chain::protocol_state_multi_index)

FC_REFLECT(eosio::chain::protocol_state_object,
		   (whitelisted_intrinsics)
)

