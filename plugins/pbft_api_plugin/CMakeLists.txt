file(GLOB HEADERS "include/eosio/pbft_api_plugin/*.hpp")
add_library( pbft_api_plugin
             pbft_api_plugin.cpp
             ${HEADERS} )

target_link_libraries( pbft_api_plugin pbft_plugin http_plugin appbase )
target_include_directories( pbft_api_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
