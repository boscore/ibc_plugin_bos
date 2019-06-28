# pbft_api_plugin


## Description
**pbft_api_plugin** exposes functionality from the pbft_plugin to the RPC API interface managed by the http_plugin, this plugin provides a chance to recover from a disaster ASAP.  

**!!! SHOULD ONLY BE USED ON SECURE NETWORKS**  


## Dependencies
**chain_plugin**  
**pbft_plugin**  
**http_plugin**


## Usage & API reference

  # config.ini  
  plugin = eosio::pbft_api_plugin  

  # nodeos startup params  
  --plugin eosio::pbft_api_plugin  

* **get_pbft_record**  

    -- To obtain pbft database state on a specified block id

    ```
    curl --request POST --data string --url http://localhost:8888/v1/pbft/get_pbft_record 
    ```    

* **get_pbft_checkpoints_record**  

    -- To obtain pbft checkpoints on a specified block number
    
    ```
    curl --request POST --data uint32_t --url http://localhost:8888/v1/pbft/get_pbft_checkpoints_record   
    ```
* **get_view_change_record**  

    -- To obtain pbft view change messages on a specified target view
    
    ```
    curl --request POST --data uint32_t --url http://localhost:8888/v1/pbft/get_view_change_record  
    ```
* **get_watermarks**  

    -- To obtain pbft high watermarks
    
    ```
    curl --request POST --url http://localhost:8888/v1/pbft/get_watermarks 
    ```
* **get_fork_schedules**  

    -- To obtain all possible producer schedules inside fork database
    
    ```
    curl --request POST --url http://localhost:8888/v1/pbft/get_fork_schedules   
    ```
* **get_pbft_status**

    -- To obtain current status from pbft state machine  
    
    ```
    curl --request POST --url http://localhost:8888/v1/pbft/get_pbft_status 
    ```
* **get_pbft_prepared_id**

    -- To obtain current prepared block id  
    
    ```
    curl --request POST --url http://localhost:8888/v1/pbft/get_pbft_prepared_id 
    ```
* **get_pbft_my_prepare_id**

    -- To obtain the block id that I recently prepare at   
    
    ```
    curl --request POST --url http://localhost:8888/v1/pbft/get_pbft_my_prepare_id 
    ```
* **set_pbft_current_view**

    -- To set the value of the current pbft view on my node, the node will transit to view change state afterwards  
    
    ```
    curl --request POST --data uint32_t --url http://localhost:8888/v1/pbft/set_pbft_current_view  
    ```