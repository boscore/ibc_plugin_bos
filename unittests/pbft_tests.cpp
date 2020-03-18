#include <boost/test/unit_test.hpp>
#include <eosio/testing/tester.hpp>
#include <eosio/chain/abi_serializer.hpp>
#include <eosio/chain/fork_database.hpp>
#include <eosio/chain/pbft_database.hpp>
#include <eosio/chain/global_property_object.hpp>

#include <contracts.hpp>

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
    controller &ctrl = *tester.control;
    pbft_controller pbft_ctrl{ctrl};

    tester.produce_block();
    auto p = pbft_ctrl.pbft_db.should_prepared();
    BOOST_CHECK(!p);
}

BOOST_AUTO_TEST_CASE(can_advance_lib_in_old_version) {
    tester tester;
    controller &ctrl = *tester.control;
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
    controller &ctrl = *tester.control;
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
    controller &ctrl = *tester.control;
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


    for(int i = 0; i< pbft_ctrl.pbft_db.get_view_change_timeout(); i++){
        pbft_ctrl.maybe_pbft_view_change();
    }
        pbft_ctrl.state_machine.do_send_view_change();
    auto new_view = pbft_ctrl.pbft_db.get_proposed_new_view_num();
    auto vcc = pbft_ctrl.pbft_db.generate_view_changed_certificate(new_view);
    auto nv_msg = pbft_ctrl.pbft_db.generate_pbft_new_view(vcc, new_view);

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
    controller &ctrl_short_prepared_fork = *short_prepared_fork.control;
    pbft_controller pbft_short_prepared_fork{ctrl_short_prepared_fork};
    controller &ctrl_long_non_prepared_fork = *long_non_prepared_fork.control;
    pbft_controller pbft_long_non_prepared_fork{ctrl_long_non_prepared_fork};
    controller &ctrl_new_view_generator = *new_view_generator.control;
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
        pbft_new_view_generator.state_machine.set_prepare_cache(pbft_prepare());
    BOOST_CHECK_EQUAL(pbft_new_view_generator.pbft_db.should_send_pbft_msg(), true);
    ctrl_new_view_generator.reset_pbft_my_prepare();
    pbft_new_view_generator.maybe_pbft_prepare();
    BOOST_CHECK_EQUAL(pbft_new_view_generator.pbft_db.should_prepared(), true);
    BOOST_CHECK_EQUAL(ctrl_new_view_generator.head_block_num(), 136);
    for(int i = 0; i < pbft_new_view_generator.pbft_db.get_view_change_timeout(); i++){
        pbft_new_view_generator.maybe_pbft_view_change();
    }
    pbft_new_view_generator.state_machine.do_send_view_change();
    auto new_view = pbft_new_view_generator.pbft_db.get_proposed_new_view_num();
    auto vcc = pbft_new_view_generator.pbft_db.generate_view_changed_certificate(new_view);
    auto nv_msg = pbft_new_view_generator.pbft_db.generate_pbft_new_view(
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
    auto pmm = pbft_message_metadata<pbft_new_view>(nv_msg, pbft_short_prepared_fork.pbft_db.get_chain_id());
    pbft_short_prepared_fork.on_pbft_new_view(std::make_shared<pbft_message_metadata<pbft_new_view>>(pmm));

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

BOOST_AUTO_TEST_CASE(new_view_with_committed_cert_call_two_times_maybe_switch_forks) {
	tester c1, new_view_generator;
	controller &c1_ctrl = *c1.control.get();
	pbft_controller c1_pbft_controller{c1_ctrl};
	controller &ctrl_new_view_generator = *new_view_generator.control.get();
	pbft_controller pbft_new_view_generator{ctrl_new_view_generator};

	auto msp = make_signature_provider();
	c1_ctrl.set_my_signature_providers(msp);
	ctrl_new_view_generator.set_my_signature_providers(msp);

	c1_ctrl.set_upo(48);
	ctrl_new_view_generator.set_upo(48);

	c1.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
	c1.set_producers({N(alice),N(bob),N(carol),N(deny)});
	c1.produce_blocks(100);

	new_view_generator.create_accounts( {N(alice),N(bob),N(carol),N(deny)} );
	new_view_generator.set_producers({N(alice),N(bob),N(carol),N(deny)});
	new_view_generator.produce_blocks(100);

	c1_pbft_controller.maybe_pbft_prepare();
	c1_pbft_controller.maybe_pbft_commit();
	c1.produce_blocks(1);
	c1_pbft_controller.maybe_pbft_commit();
	c1.produce_blocks(25);

	pbft_new_view_generator.maybe_pbft_prepare();
	pbft_new_view_generator.maybe_pbft_commit();
	new_view_generator.produce_blocks(1);
	pbft_new_view_generator.maybe_pbft_commit();
	new_view_generator.produce_blocks(25);

	BOOST_CHECK_EQUAL(ctrl_new_view_generator.head_block_num(), 127);
	BOOST_CHECK_EQUAL(c1_ctrl.head_block_num(), 127);
	BOOST_CHECK_EQUAL(ctrl_new_view_generator.last_irreversible_block_num(), 101);
	BOOST_CHECK_EQUAL(c1_ctrl.last_irreversible_block_num(), 101);

	c1.create_accounts({N(shortname)});
	new_view_generator.create_accounts({N(longname)});
	c1.produce_blocks(6);
	new_view_generator.produce_blocks(10);

        c1_pbft_controller.state_machine.set_prepare_cache(pbft_prepare());
	c1_ctrl.reset_pbft_my_prepare();
	c1_pbft_controller.maybe_pbft_prepare();
	c1.produce_block();

	//merge short fork and long fork, make sure current head is short fork
	for(int i=1;i<=10;i++){
		auto tmp = ctrl_new_view_generator.fetch_block_by_number(127+i);
		c1.push_block(tmp);
	}
	BOOST_CHECK_EQUAL(c1_ctrl.head_block_num(), 134);
	auto c1_my_prepare = c1_ctrl.get_pbft_my_prepare();
	auto c1_my_prepare_num = c1_ctrl.fork_db().get_block(c1_my_prepare)->block_num;
	BOOST_CHECK_EQUAL(c1_my_prepare_num, 133);


	//generate new view with long fork commit certificate
        pbft_new_view_generator.state_machine.set_prepare_cache(pbft_prepare());
	BOOST_CHECK_EQUAL(pbft_new_view_generator.pbft_db.should_send_pbft_msg(), true);
	ctrl_new_view_generator.reset_pbft_my_prepare();
	pbft_new_view_generator.maybe_pbft_prepare();

	auto pbft_new_view_prepare = ctrl_new_view_generator.get_pbft_my_prepare();
	auto pbft_new_view_prepare_num = ctrl_new_view_generator.fork_db().get_block(pbft_new_view_prepare)->block_num;
	BOOST_CHECK_EQUAL(pbft_new_view_prepare_num, 137);

	BOOST_CHECK_EQUAL(ctrl_new_view_generator.head_block_num(), 137);
	pbft_new_view_generator.maybe_pbft_commit();
	new_view_generator.produce_block();
	BOOST_CHECK_EQUAL(ctrl_new_view_generator.last_irreversible_block_num(), 137);

	for(int i = 0; i < pbft_new_view_generator.pbft_db.get_view_change_timeout(); i++){
		pbft_new_view_generator.maybe_pbft_view_change();
	}
	pbft_new_view_generator.state_machine.do_send_view_change();
	auto new_view = pbft_new_view_generator.pbft_db.get_proposed_new_view_num();
	auto vcc = pbft_new_view_generator.pbft_db.generate_view_changed_certificate(new_view);
	auto nv_msg = pbft_new_view_generator.pbft_db.generate_pbft_new_view(vcc, new_view);

	//can switch fork after apply prepare certificate in new view
	auto pmm = pbft_message_metadata<pbft_new_view>(nv_msg, c1_pbft_controller.pbft_db.get_chain_id());

	c1_pbft_controller.on_pbft_new_view(std::make_shared<pbft_message_metadata<pbft_new_view>>(pmm));
	c1_pbft_controller.maybe_pbft_commit();
	c1.produce_blocks(2);
	BOOST_CHECK_EQUAL(c1_ctrl.last_irreversible_block_num(), 137);
	// make sure committed block same with new view generator lib block
	BOOST_CHECK_EQUAL(c1_ctrl.fetch_block_by_number(137)->id(), ctrl_new_view_generator.fetch_block_by_number(137)->id());
}


private_key_type get_private_key( name keyname, string role ) {
	return private_key_type::regenerate<fc::ecc::private_key_shim>(fc::sha256::hash(string(keyname)+role));
}

public_key_type  get_public_key( name keyname, string role ){
	return get_private_key( keyname, role ).get_public_key();
}

BOOST_AUTO_TEST_CASE(switch_fork_reserve_prepare) {
	const char* curr_state;
	tester c1, c2, c2_prime, c3_final;
	controller &c1_ctrl = *c1.control.get(); /// only have signature Alice
	pbft_controller c1_pbft_controller{c1_ctrl};
	controller &c2_ctrl = *c2.control.get(); /// have signature Alice and bob
	pbft_controller c2_pbft_controller{c2_ctrl};
	controller &c2_ctrl_prime = c2_ctrl;     /// for make fork
	pbft_controller c2_prime_pbft_controller{c2_ctrl_prime};
	controller &c3_final_ctrl = *c3_final.control.get(); /// only have signature bob
	pbft_controller c3_final_pbft_controller{c3_final_ctrl};


	c1_ctrl.set_upo(48);
	c2_ctrl.set_upo(48);
	c2_ctrl_prime.set_upo(48);
	c3_final_ctrl.set_upo(48);

	/// c1
	c1.produce_block();
	c1.create_accounts( {N(alice), N(bob)} );
	c1.produce_block();

	// make and get signature provider for c1
	std::map<eosio::chain::public_key_type, signature_provider_type> msp_c1;
	auto priv_alice = tester::get_private_key( N(alice), "active" );
	auto pub_alice = tester::get_public_key( N(alice), "active");
	auto sp_alice = [priv_alice]( const eosio::chain::digest_type& digest ) {
		return priv_alice.sign(digest);
	};
	msp_c1[pub_alice]=sp_alice;
	c1_ctrl.set_my_signature_providers(msp_c1);

	c1.set_producers( {N(alice), N(bob)} );

	vector<producer_key> c1_sch = { {N(alice),get_public_key(N(alice), "active")} };


	/// c2
	c2.produce_block();
	c2.create_accounts( {N(alice), N(bob)} );
	c2.produce_block();

	// make and get signature provider for c2
	std::map<eosio::chain::public_key_type, signature_provider_type> msp_c2;
	msp_c2[pub_alice]=sp_alice;

	auto priv_bob = tester::get_private_key( N(bob), "active" );
	auto pub_bob = tester::get_public_key( N(bob), "active");
	auto sp_bob = [priv_bob]( const eosio::chain::digest_type& digest ) {
		return priv_bob.sign(digest);
	};
	msp_c2[pub_bob]=sp_bob;
	c2_ctrl.set_my_signature_providers(msp_c2);

	c2.set_producers( {N(alice), N(bob)} );

	vector<producer_key> c2_sch = { {N(alice),get_public_key(N(alice), "active")},
									{N(bob),get_public_key(N(bob), "active")}};


	c2.produce_blocks(95);
	c2_pbft_controller.maybe_pbft_prepare();
	c2_pbft_controller.maybe_pbft_commit();
	c2.produce_block();
	BOOST_CHECK_EQUAL(c2.control->last_irreversible_block_num(), 98);

	/// c3 final
	c3_final.produce_block();
	c3_final.create_accounts( {N(alice), N(bob)} );
	c3_final.produce_block();

	// make and get signature provider for c3 final
	std::map<eosio::chain::public_key_type, signature_provider_type> msp_c3_final;
	msp_c3_final[pub_bob]=sp_bob;

	c3_final_ctrl.set_my_signature_providers(msp_c3_final);

	c3_final.set_producers( {N(alice), N(bob)} );

	vector<producer_key> c3_final_sch = { {N(bob),get_public_key(N(bob), "active")} };

	push_blocks(c2, c3_final);


	push_blocks(c2, c1);
	/// make c1 lib 98
	c1_pbft_controller.maybe_pbft_prepare();
	pbft_prepare c2_prepare_ = c2_pbft_controller.state_machine.get_prepare_cache();
	BOOST_CHECK_EQUAL(c2_prepare_.block_info.block_num(), 98);
	c1_pbft_controller.state_machine.on_prepare(std::make_shared<pbft_message_metadata<pbft_prepare>>(c2_prepare_, c2_pbft_controller.pbft_db.get_chain_id()));
	c1_pbft_controller.maybe_pbft_commit();

	pbft_commit c2_commit_ = c2_pbft_controller.state_machine.get_commit_cache();
	c1_pbft_controller.state_machine.on_commit(std::make_shared<pbft_message_metadata<pbft_commit>>(c2_commit_, c2_pbft_controller.pbft_db.get_chain_id()));
	auto c1_my_prepare_block = c1_ctrl.get_pbft_my_prepare();
	c1.produce_block();
	c1_pbft_controller.maybe_pbft_commit();
	BOOST_CHECK_EQUAL(c1.control->last_irreversible_block_num(), 98);
//		c1_ctrl.set_pbft_my_prepare(c1_ctrl.get_block_id_for_num(99));

	c2_pbft_controller.maybe_pbft_commit();

	/// make c3_final lib 98
	c3_final_pbft_controller.maybe_pbft_prepare();
	pbft_prepare c1_prepare_ = c1_pbft_controller.state_machine.get_prepare_cache();

	auto forks = c1_ctrl.fork_db().fetch_branch_from(c1_prepare_.block_info.block_id, c1_my_prepare_block);
	BOOST_CHECK_EQUAL(forks.first.size() >= 1 && forks.second.size() == 1, true);
	c3_final.produce_block();
	c3_final_pbft_controller.state_machine.on_prepare(std::make_shared<pbft_message_metadata<pbft_prepare>>(c1_prepare_, c1_pbft_controller.pbft_db.get_chain_id()));
	c3_final_pbft_controller.maybe_pbft_commit();

	pbft_commit c1_commit_ = c1_pbft_controller.state_machine.get_commit_cache();
	c3_final_pbft_controller.state_machine.on_commit(std::make_shared<pbft_message_metadata<pbft_commit>>(c1_commit_, c1_pbft_controller.pbft_db.get_chain_id()));
	c3_final.produce_block();
	c3_final_pbft_controller.maybe_pbft_commit();
	BOOST_CHECK_EQUAL(c3_final.control->last_irreversible_block_num(), 98);

	// copy c2 to c2_prime
	push_blocks(c2, c2_prime);

	pbft_prepare c1_prepare;
	c1_prepare_ = c1_pbft_controller.state_machine.get_prepare_cache();
	c1_prepare = c1_prepare_;

	/// for set pbft commit cache to 99
	c2.produce_block();

	c2_pbft_controller.maybe_pbft_prepare();
	c2_prepare_ = c2_pbft_controller.state_machine.get_prepare_cache();
	BOOST_CHECK_EQUAL(c2_prepare_.block_info.block_num(), 99);
	c2_pbft_controller.maybe_pbft_commit();
	c2.produce_block();
	c2_pbft_controller.maybe_pbft_commit();

	c2_commit_ = c2_pbft_controller.state_machine.get_commit_cache();
	BOOST_CHECK_EQUAL(c2_commit_.block_info.block_num(), 99);
	BOOST_CHECK_EQUAL(c2.control->last_irreversible_block_num(), 99);

	push_blocks(c2, c1);
	c1_pbft_controller.maybe_pbft_prepare();
	c2_prepare_ = c2_pbft_controller.state_machine.get_prepare_cache();
	c1_pbft_controller.state_machine.on_prepare(std::make_shared<pbft_message_metadata<pbft_prepare>>(c2_prepare_, c2_pbft_controller.pbft_db.get_chain_id()));
	c1_pbft_controller.maybe_pbft_commit();
	c2_commit_ = c2_pbft_controller.state_machine.get_commit_cache();
	c1_pbft_controller.state_machine.on_commit(std::make_shared<pbft_message_metadata<pbft_commit>>(c2_commit_, c2_pbft_controller.pbft_db.get_chain_id()));
	c1.produce_block();
	c1_pbft_controller.maybe_pbft_commit();

	c1_prepare_ = c1_pbft_controller.state_machine.get_prepare_cache();
	c1_commit_ = c1_pbft_controller.state_machine.get_commit_cache();
	BOOST_CHECK_EQUAL(c1_commit_.block_info.block_num(), 100);
	BOOST_CHECK_EQUAL(c1.control->last_irreversible_block_num(), 99);

	c3_final_pbft_controller.maybe_pbft_prepare();

	c3_final.produce_block();
	c3_final.produce_block();

	BOOST_CHECK_EQUAL(c1_prepare.block_info.block_num(), 100);
	/// set c3 my prepare at 101
	c3_final_ctrl.set_pbft_my_prepare(c3_final_ctrl.get_block_id_for_num(101));
	c3_final_pbft_controller.state_machine.on_prepare(std::make_shared<pbft_message_metadata<pbft_prepare>>(c1_prepare, c1_pbft_controller.pbft_db.get_chain_id()));

	pbft_prepare c3_final_prepare = c3_final_pbft_controller.state_machine.get_prepare_cache();
	// check c3 prepare at 101
	BOOST_CHECK_EQUAL(c3_final_prepare.block_info.block_num(), 101);
	curr_state = c3_final_pbft_controller.state_machine.get_current()->get_name();
	BOOST_CHECK_EQUAL(curr_state, "{==== PREPARED ====}");


	c3_final_pbft_controller.maybe_pbft_commit();

	c2_prime.produce_block();
	c2_prime.create_accounts({N(tester1)});
	c2_prime.produce_blocks(6);

	//push fork to c3_final
	for(int i = 101; i <= 106; i++) {
		auto fb = c2_prime.control->fetch_block_by_number(i);
		c3_final.push_block(fb);
	}

	auto tmp_c2_prime_prepared_fork = c3_final.control->fork_db().get_block(c2_prime.control->get_block_id_for_num(100));
	auto tmp_c3_final_prepared_fork = c3_final.control->fork_db().get_block(c3_final.control->get_block_id_for_num(100));
	BOOST_CHECK_EQUAL(tmp_c3_final_prepared_fork->pbft_prepared, true);
	BOOST_CHECK_EQUAL(tmp_c3_final_prepared_fork->pbft_my_prepare, true);
	BOOST_CHECK_EQUAL(tmp_c2_prime_prepared_fork->pbft_prepared, true);
	BOOST_CHECK_EQUAL(tmp_c2_prime_prepared_fork->pbft_my_prepare, true);
	BOOST_CHECK_EQUAL(c3_final.control->last_irreversible_block_num(), 98);

	BOOST_CHECK_EQUAL(c3_final.control->head_block_num(), 106);

	c3_final_pbft_controller.maybe_pbft_commit();
	c3_final.produce_block();
	pbft_commit c3_final_commit = c3_final_pbft_controller.state_machine.get_commit_cache();
	/// on commit will prepare next block immediately will trigger reserve prepare
	c3_final_pbft_controller.state_machine.on_commit(std::make_shared<pbft_message_metadata<pbft_commit>>(c1_commit_, c1_pbft_controller.pbft_db.get_chain_id()));
	// for sync
	c3_final.produce_block();
	c3_final_pbft_controller.maybe_pbft_commit();
	curr_state = c3_final_pbft_controller.state_machine.get_current()->get_name();
	BOOST_CHECK_EQUAL(curr_state, "{==== COMMITTED ====}");
	BOOST_CHECK_EQUAL(c3_final.control->last_irreversible_block_num(), 100);

	c3_final_pbft_controller.maybe_pbft_prepare();
	c3_final_prepare = c3_final_pbft_controller.state_machine.get_prepare_cache();
	BOOST_CHECK_EQUAL(c3_final_prepare.block_info.block_num(), 101);

}


BOOST_AUTO_TEST_CASE(fetch_branch_from_block_at_same_fork_return_at_least_common_ancestor) {
	//create forks
	tester c;
	c.produce_block();
	c.produce_block();
	auto r = c.create_accounts( {N(dan),N(sam),N(pam)} );
	wdump((fc::json::to_pretty_string(r)));
	c.produce_block();
	auto res = c.set_producers( {N(dan),N(sam),N(pam)} );
	vector<producer_key> sch = { {N(dan),get_public_key(N(dan), "active")},
								 {N(sam),get_public_key(N(sam), "active")},
								 {N(pam),get_public_key(N(pam), "active")}};
	wdump((fc::json::to_pretty_string(res)));
	wlog("set producer schedule to [dan,sam,pam]");
	c.produce_blocks(98);

	BOOST_CHECK_EQUAL(c.control->head_block_num(), 102);

	// first is low block number
	auto first = c.control->get_block_id_for_num(100);
	auto second = c.control->get_block_id_for_num(102);
	auto result = c.control->fork_db().fetch_branch_from(first, second);

	BOOST_CHECK_EQUAL(result.first.size(), 1);
	BOOST_CHECK_EQUAL(result.second.size(), 3);


	// first is high block number
	first = c.control->get_block_id_for_num(102);
	second = c.control->get_block_id_for_num(99);
	result = c.control->fork_db().fetch_branch_from(first, second);

	BOOST_CHECK_EQUAL(result.first.size(), 4);
	BOOST_CHECK_EQUAL(result.second.size(), 1);

	BOOST_CHECK_EQUAL(result.first[result.first.size() - 1]->id, result.second[result.second.size() - 1]->id);
}


BOOST_AUTO_TEST_CASE(fetch_branch_from_block_at_same_block_return_at_least_common_ancestor) {
	//create forks
	tester c;
	c.produce_block();
	c.produce_block();
	auto r = c.create_accounts( {N(dan),N(sam),N(pam)} );
	wdump((fc::json::to_pretty_string(r)));
	c.produce_block();
	auto res = c.set_producers( {N(dan),N(sam),N(pam)} );
	vector<producer_key> sch = { {N(dan),get_public_key(N(dan), "active")},
								 {N(sam),get_public_key(N(sam), "active")},
								 {N(pam),get_public_key(N(pam), "active")}};
	wdump((fc::json::to_pretty_string(res)));
	wlog("set producer schedule to [dan,sam,pam]");
	c.produce_blocks(98);

	BOOST_CHECK_EQUAL(c.control->head_block_num(), 102);

	// same block
	auto first = c.control->get_block_id_for_num(102);
	auto second = c.control->get_block_id_for_num(102);
	auto result = c.control->fork_db().fetch_branch_from(first, second);

	// result two branch will only include one common_ancestor
	BOOST_CHECK_EQUAL(result.first.size(), 1);
	BOOST_CHECK_EQUAL(result.second.size(), 1);

	BOOST_CHECK_EQUAL(result.first[result.first.size() - 1]->id == result.second[result.second.size() - 1]->id, true);
}


BOOST_AUTO_TEST_CASE(fetch_branch_from_block_at_diffent_fork_return_without_common_ancestor) {
	//create forks
	tester c;
	c.produce_block();
	c.produce_block();
	auto r = c.create_accounts( {N(dan),N(sam),N(pam)} );
	wdump((fc::json::to_pretty_string(r)));
	c.produce_block();
	auto res = c.set_producers( {N(dan),N(sam),N(pam)} );
	vector<producer_key> sch = { {N(dan),get_public_key(N(dan), "active")},
								 {N(sam),get_public_key(N(sam), "active")},
								 {N(pam),get_public_key(N(pam), "active")}};
	wdump((fc::json::to_pretty_string(res)));
	wlog("set producer schedule to [dan,sam,pam]");
	c.produce_blocks(98);

	tester c2;
	c2.produce_block();
	c2.produce_block();
	c2.create_accounts( {N(dan),N(sam),N(pam)} );
	c2.produce_block();
	c2.set_producers( {N(dan),N(sam),N(pam)} );
	wlog("set producer schedule to [dan,sam,pam]");
	c2.produce_blocks(98);

	//check c2 98 c98 equal

	c.create_accounts({N(tester1)});
	c.produce_blocks(10);
	c2.create_accounts({N(tester2)});
	c2.produce_blocks(1);

	auto c2_previous_fork_head_id = c2.control->get_block_id_for_num(103);
	auto c2_tmp_102 = c2.control->get_block_id_for_num(102);
	BOOST_CHECK_EQUAL(c2.control->head_block_num(), 103);

	// push this fork to c2
	for (int i = 103; i <= 104; i++) {
		auto fb = c.control->fetch_block_by_number(i);
		c2.push_block(fb);
	}

	BOOST_CHECK_EQUAL(c2.control->head_block_num(), 104);


	auto first = c2.control->get_block_id_for_num(103);
	BOOST_CHECK_EQUAL(first!= c2_previous_fork_head_id, true);
	BOOST_CHECK_EQUAL(c2.control->get_block_id_for_num(102) == c2_tmp_102, true);
	auto result = c2.control->fork_db().fetch_branch_from(first, c2_previous_fork_head_id);

	// if first and second in different fork, the result two branch do not include common ancestor
	BOOST_CHECK_EQUAL(result.first.size(), 1);
	BOOST_CHECK_EQUAL(result.second.size(), 1);

	BOOST_CHECK_EQUAL(result.first[result.first.size() - 1]->id != result.second[result.second.size() - 1]->id, true);
	BOOST_CHECK_EQUAL(result.first[result.first.size() - 1]->block_num, 103);
	BOOST_CHECK_EQUAL(result.second[result.second.size() - 1]->block_num, 103);
}

BOOST_AUTO_TEST_SUITE_END()
