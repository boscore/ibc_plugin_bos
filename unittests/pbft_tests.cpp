#include <boost/test/unit_test.hpp>
#include <eosio/testing/tester.hpp>
#include <eosio/chain/abi_serializer.hpp>
#include <eosio/chain/fork_database.hpp>
#include <eosio/chain/pbft_database.hpp>
#include <eosio/chain/global_property_object.hpp>

#include <eosio.token/eosio.token.wast.hpp>
#include <eosio.token/eosio.token.abi.hpp>

#include <Runtime/Runtime.h>

#include <fc/variant_object.hpp>

using namespace eosio::chain;
using namespace eosio::testing;


BOOST_AUTO_TEST_SUITE(pbft_tests)
std::map<eosio::chain::public_key_type, signature_provider_type> make_signature_provider(){
    std::map<eosio::chain::public_key_type, signature_provider_type> msp;

    auto priv_eosio = tester::get_private_key( N(eosio), "active" );
    auto pub_eosio = tester::get_public_key( N(eosio), "active");
    auto sp_eosio = [priv_eosio]( const eosio::chain::digest_type& digest ) {
      return priv_eosio.sign(digest);
    };
    msp[pub_eosio]=sp_eosio;

    auto priv_alice = tester::get_private_key( N(alice), "active" );
    auto pub_alice = tester::get_public_key( N(alice), "active");
    auto sp_alice = [priv_alice]( const eosio::chain::digest_type& digest ) {
      return priv_alice.sign(digest);
    };
    msp[pub_alice]=sp_alice;

    auto priv_bob = tester::get_private_key( N(bob), "active" );
    auto pub_bob = tester::get_public_key( N(bob), "active");
    auto sp_bob = [priv_bob]( const eosio::chain::digest_type& digest ) {
      return priv_bob.sign(digest);
    };
    msp[pub_bob]=sp_bob;

    auto priv_carol = tester::get_private_key( N(carol), "active" );
    auto pub_carol = tester::get_public_key( N(carol), "active");
    auto sp_carol = [priv_carol]( const eosio::chain::digest_type& digest ) {
      return priv_carol.sign(digest);
    };
    msp[pub_carol]=sp_carol;

    auto priv_deny = tester::get_private_key( N(deny), "active" );
    auto pub_deny = tester::get_public_key( N(deny), "active");
    auto sp_deny = [priv_deny]( const eosio::chain::digest_type& digest ) {
      return priv_deny.sign(digest);
    };
    msp[pub_deny]=sp_deny;

    return msp;
}

BOOST_AUTO_TEST_CASE(can_init) {
    tester tester;
    controller &ctrl = *tester.control.get();
    pbft_controller pbft_ctrl{ctrl};

    tester.produce_block();
    auto p = pbft_ctrl.pbft_db.should_prepared();
    BOOST_CHECK(!p);
}

BOOST_AUTO_TEST_CASE(can_advance_lib_in_old_version) {
    tester tester;
    controller &ctrl = *tester.control.get();
    pbft_controller pbft_ctrl{ctrl};

    auto msp = make_signature_provider();
    ctrl.set_my_signature_providers(msp);

    tester.produce_block();//produce block num 2
    BOOST_REQUIRE_EQUAL(ctrl.last_irreversible_block_num(), 0);
    BOOST_REQUIRE_EQUAL(ctrl.head_block_num(), 2);
    tester.produce_block();
    BOOST_REQUIRE_EQUAL(ctrl.last_irreversible_block_num(), 2);
    BOOST_REQUIRE_EQUAL(ctrl.head_block_num(), 3);
    }

BOOST_AUTO_TEST_CASE(can_advance_lib_after_upgrade) {
    tester tester;
    controller &ctrl = *tester.control.get();
    pbft_controller pbft_ctrl{ctrl};
    ctrl.set_upo(150);

    const auto& upo = ctrl.db().get<upgrade_property_object>();
    const auto upo_upgrade_target_block_num = upo.upgrade_target_block_num;
    BOOST_CHECK_EQUAL(upo_upgrade_target_block_num, 150);

    auto msp = make_signature_provider();
    ctrl.set_my_signature_providers(msp);

    auto is_upgraded = ctrl.is_pbft_enabled();

    BOOST_CHECK_EQUAL(is_upgraded, false);

    tester.produce_block();//produce block num 2
    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 0);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 2);
    tester.produce_blocks(150);
    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 151);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 152);

    is_upgraded = ctrl.is_pbft_enabled();
    BOOST_CHECK_EQUAL(is_upgraded, true);

    tester.produce_blocks(10);
    BOOST_CHECK_EQUAL(ctrl.pending_pbft_lib(), false);
    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 151);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 162);

    pbft_ctrl.maybe_pbft_prepare();
    pbft_ctrl.maybe_pbft_commit();

    BOOST_CHECK_EQUAL(ctrl.pending_pbft_lib(), true);
    tester.produce_block(); //set lib using pending pbft lib

    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 162);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 163);
}



BOOST_AUTO_TEST_CASE(can_advance_lib_after_upgrade_with_four_producers) {
    tester tester;
    controller &ctrl = *tester.control.get();
    pbft_controller pbft_ctrl{ctrl};

    ctrl.set_upo(109);

    const auto& upo = ctrl.db().get<upgrade_property_object>();
    const auto upo_upgrade_target_block_num = upo.upgrade_target_block_num;
    BOOST_CHECK_EQUAL(upo_upgrade_target_block_num, 109);

    auto msp = make_signature_provider();
    ctrl.set_my_signature_providers(msp);

    auto is_upgraded = ctrl.is_pbft_enabled();

    BOOST_CHECK_EQUAL(is_upgraded, false);

    tester.produce_block();//produce block num 2
    tester.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
    tester.set_producers({N(alice),N(bob),N(carol),N(deny)});
    tester.produce_blocks(3);//produce block num 3,4,5
    BOOST_CHECK_EQUAL(ctrl.active_producers().producers.front().producer_name, N(alice));
    BOOST_CHECK_EQUAL(ctrl.head_block_producer(),N(eosio));
    tester.produce_blocks(7);//produce to block 12
    BOOST_CHECK_EQUAL(ctrl.head_block_producer(),N(alice));

    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 4);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 12);
    tester.produce_blocks(156 - 12);
    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 108);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 156);

    is_upgraded = ctrl.is_pbft_enabled();
    BOOST_CHECK_EQUAL(is_upgraded, false);
    tester.produce_blocks(12);
    is_upgraded = ctrl.is_pbft_enabled();
    BOOST_CHECK_EQUAL(is_upgraded, true);
    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 120);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 168);
    BOOST_CHECK_EQUAL(ctrl.pending_pbft_lib(), false);

    pbft_ctrl.maybe_pbft_prepare();
    pbft_ctrl.maybe_pbft_commit();

    BOOST_CHECK_EQUAL(ctrl.pending_pbft_lib(), true);
    tester.produce_block(); //set lib using pending pbft lib

    BOOST_CHECK_EQUAL(ctrl.last_irreversible_block_num(), 168);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 169);
}

void push_blocks( tester& from, tester& to ) {
    while( to.control->fork_db_head_block_num() < from.control->fork_db_head_block_num() ) {
        auto fb = from.control->fetch_block_by_number( to.control->fork_db_head_block_num()+1 );
        to.push_block( fb );
    }
}

BOOST_AUTO_TEST_CASE(view_change_validation) {
    tester tester;
    controller &ctrl = *tester.control;
    pbft_controller pbft_ctrl{ctrl};

    auto msp = make_signature_provider();
    ctrl.set_my_signature_providers(msp);

    ctrl.set_upo(48);

    tester.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
    tester.set_producers({N(alice),N(bob),N(carol),N(deny)});
    tester.produce_blocks(100);

    pbft_ctrl.maybe_pbft_prepare();
    pbft_ctrl.maybe_pbft_commit();
    tester.produce_blocks(1);

    BOOST_CHECK_EQUAL(ctrl.is_pbft_enabled(), true);
    BOOST_CHECK_EQUAL(ctrl.head_block_num(), 102);


    for(int i = 0; i< pbft_ctrl.view_change_timeout; i++){
        pbft_ctrl.maybe_pbft_view_change();
    }
        pbft_ctrl.state_machine->do_send_view_change();
    auto new_view = pbft_ctrl.pbft_db.get_proposed_new_view_num();
    auto vcc = pbft_ctrl.pbft_db.generate_view_changed_certificate(new_view);
    auto nv_msg = pbft_ctrl.pbft_db.send_pbft_new_view(vcc, new_view);

    bool nv_flag;
    try {
        pbft_ctrl.pbft_db.validate_new_view(nv_msg, tester::get_public_key(N(carol), "active"));
        nv_flag = true;
    } catch (fc::exception &e) {
        nv_flag = false;
    }
    BOOST_CHECK_EQUAL(nv_flag, false);
}

BOOST_AUTO_TEST_CASE(switch_fork_when_accept_new_view_with_prepare_certificate_on_short_fork) {
    tester short_prepared_fork, long_non_prepared_fork, new_view_generator;
    controller &ctrl_short_prepared_fork = *short_prepared_fork.control.get();
    pbft_controller pbft_short_prepared_fork{ctrl_short_prepared_fork};
    controller &ctrl_long_non_prepared_fork = *long_non_prepared_fork.control.get();
    pbft_controller pbft_long_non_prepared_fork{ctrl_long_non_prepared_fork};
    controller &ctrl_new_view_generator = *new_view_generator.control.get();
    pbft_controller pbft_new_view_generator{ctrl_new_view_generator};

    auto msp = make_signature_provider();
    ctrl_short_prepared_fork.set_my_signature_providers(msp);
    ctrl_long_non_prepared_fork.set_my_signature_providers(msp);
    ctrl_new_view_generator.set_my_signature_providers(msp);

    ctrl_short_prepared_fork.set_upo(48);
    ctrl_long_non_prepared_fork.set_upo(48);
    ctrl_new_view_generator.set_upo(48);

    long_non_prepared_fork.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
    long_non_prepared_fork.set_producers({N(alice),N(bob),N(carol),N(deny)});
    long_non_prepared_fork.produce_blocks(100);

    short_prepared_fork.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
    short_prepared_fork.set_producers({N(alice),N(bob),N(carol),N(deny)});
    short_prepared_fork.produce_blocks(100);

    new_view_generator.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
    new_view_generator.set_producers({N(alice),N(bob),N(carol),N(deny)});
    new_view_generator.produce_blocks(100);

    pbft_long_non_prepared_fork.maybe_pbft_prepare();
    pbft_long_non_prepared_fork.maybe_pbft_commit();
    long_non_prepared_fork.produce_blocks(1);
    pbft_long_non_prepared_fork.maybe_pbft_commit();
    long_non_prepared_fork.produce_blocks(25);

    pbft_short_prepared_fork.maybe_pbft_prepare();
    pbft_short_prepared_fork.maybe_pbft_commit();
    short_prepared_fork.produce_blocks(1);
    pbft_short_prepared_fork.maybe_pbft_commit();
    short_prepared_fork.produce_blocks(25);


    pbft_new_view_generator.maybe_pbft_prepare();
    pbft_new_view_generator.maybe_pbft_commit();
    new_view_generator.produce_blocks(1);

    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.is_pbft_enabled(), true);
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.is_pbft_enabled(), true);
    BOOST_CHECK_EQUAL(ctrl_new_view_generator.is_pbft_enabled(), true);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 127);
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.head_block_num(), 127);
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.fetch_block_by_number(100)->id(), ctrl_short_prepared_fork.fetch_block_by_number(100)->id());



    short_prepared_fork.create_accounts({N(shortname)});
    long_non_prepared_fork.create_accounts({N(longname)});
    short_prepared_fork.produce_blocks(6);
    push_blocks(short_prepared_fork, new_view_generator);
    long_non_prepared_fork.produce_blocks(10);


    pbft_new_view_generator.maybe_pbft_commit();
    new_view_generator.produce_blocks(3);
    push_blocks(new_view_generator, short_prepared_fork);

    BOOST_CHECK_EQUAL(ctrl_new_view_generator.head_block_num(), 136);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 136);
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.head_block_num(),  137);
    BOOST_CHECK_EQUAL(ctrl_new_view_generator.last_irreversible_block_num(), 101);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.last_irreversible_block_num(), 101);
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.last_irreversible_block_num(), 101);

    //generate new view with short fork prepare certificate
    pbft_new_view_generator.state_machine->set_prepares_cache(pbft_prepare());
    BOOST_CHECK_EQUAL(pbft_new_view_generator.pbft_db.should_send_pbft_msg(), true);
    pbft_new_view_generator.maybe_pbft_prepare();
    BOOST_CHECK_EQUAL(pbft_new_view_generator.pbft_db.should_prepared(), true);
    BOOST_CHECK_EQUAL(ctrl_new_view_generator.head_block_num(), 136);
    for(int i = 0; i<pbft_new_view_generator.view_change_timeout; i++){
        pbft_new_view_generator.maybe_pbft_view_change();
    }
    pbft_new_view_generator.state_machine->do_send_view_change();
    auto new_view = pbft_new_view_generator.pbft_db.get_proposed_new_view_num();
    auto vcc = pbft_new_view_generator.pbft_db.generate_view_changed_certificate(new_view);
    auto nv_msg = pbft_new_view_generator.pbft_db.send_pbft_new_view(
            vcc,
            new_view);

    //merge short fork and long fork, make sure current head is long fork
    for(int i=1;i<=10;i++){
        auto tmp = ctrl_long_non_prepared_fork.fetch_block_by_number(127+i);
        short_prepared_fork.push_block(tmp);
    }
    BOOST_CHECK_EQUAL(ctrl_long_non_prepared_fork.head_block_num(), 137);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 137);


    ctrl_short_prepared_fork.reset_pbft_prepared();
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 137);

    //can switch fork after apply prepare certificate in new view
    pbft_short_prepared_fork.state_machine->on_new_view(std::make_shared<pbft_message_metadata<pbft_new_view>>(nv_msg, ctrl_new_view_generator.get_chain_id()));

    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 136);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.last_irreversible_block_num(), 101);


    //can switch fork after set lib
    ctrl_short_prepared_fork.set_pbft_prepared(ctrl_short_prepared_fork.last_irreversible_block_id());
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 137);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.last_irreversible_block_num(), 101);

    pbft_short_prepared_fork.maybe_pbft_commit();
    short_prepared_fork.produce_blocks(2);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.head_block_num(), 138);
    BOOST_CHECK_EQUAL(ctrl_short_prepared_fork.last_irreversible_block_num(), 136);
}



BOOST_AUTO_TEST_SUITE_END()
