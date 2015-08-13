#include <config.h>
#include "data_warehouse.h"
#include "../../CM_testbed_code/public_lib/sample_setting.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"

extern cm_experiment_setting_t cm_experiment_setting;

/**
* @brief init the data warehouse, at the beginning, only the first buffer should be initiliazed
*
* @return 0-succ -1:fail
*/
int data_warehouse_init(void) {
    //set the initial buffer
    int switch_idx = 0;

    int max_switch_map_size = 0;
    if (cm_experiment_setting.switch_mem_type == UNIFORM) {
        uint64_t max_switch_interval_volume = 0;
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            if (cm_experiment_setting.switches_interval_volume[switch_idx] > max_switch_interval_volume) {
                max_switch_interval_volume = cm_experiment_setting.switches_interval_volume[switch_idx];
            }
        } 
        max_switch_map_size = DEFAULT_BYTE_SAMPLE_RATE * max_switch_interval_volume ;
        if (max_switch_map_size == 0) {
            ERROR("FAIL: max_switch_map_size=0");
            return -1;
        }
    }

    int a_idx = 0;  //active buffer idx
    int a_condition_idx = 0;
    for (; a_idx < BUFFER_NUM; ++a_idx, ++a_condition_idx) {
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            data_warehouse.flow_volume_map[a_idx][switch_idx] = ht_kfs_vi_create();
            if (data_warehouse.flow_volume_map[a_idx][switch_idx] == NULL) {
                return -1;
            }
            data_warehouse.target_flow_map[a_condition_idx][switch_idx] = ht_kfs_vi_create();
            if (data_warehouse.target_flow_map[a_condition_idx][switch_idx] == NULL) {
                return -1;
            }
            if (cm_experiment_setting.switch_mem_type == UNIFORM) {
                data_warehouse.flow_sample_map[a_idx][switch_idx] = ht_kfs_vi_fixSize_create(max_switch_map_size);
            } else if (cm_experiment_setting.switch_mem_type == DIVERSE) {
                int map_size = DEFAULT_BYTE_SAMPLE_RATE * cm_experiment_setting.switches_interval_volume[switch_idx];
                data_warehouse.flow_sample_map[a_idx][switch_idx] = ht_kfs_vi_fixSize_create(map_size);
            }
            if (data_warehouse.flow_sample_map[a_idx][switch_idx] == NULL) {
                return -1;
            }
        }
    }

    pthread_mutex_init(&data_warehouse.target_flow_map_mutex, NULL);

    data_warehouse.active_idx = 0;
    data_warehouse.active_condition_idx = 0;
    NOTICE("SUCC data_warehouse_init");

    return 0;
}



/**
* @brief call when need to switch to another buffer
*/
void data_warehouse_rotate_buffer_idx(void) {
    data_warehouse.active_idx = (data_warehouse.active_idx + 1) % BUFFER_NUM;
}

/**
* @brief This should be called after data_warehouse_rotate_buffer() is called, and after non-active buffer is useless
*
*
* @return 0-succ, -1-fail
*/
int data_warehouse_reset_noactive_buf(void) {
    //noactive buffer idx
    int na_idx = (data_warehouse.active_idx + 1) % BUFFER_NUM;
    int switch_idx = 0;
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        //refresh the hashmaps
        ht_kfs_vi_refresh(data_warehouse.flow_volume_map[na_idx][switch_idx]);
        ht_kfs_vi_fixSize_refresh(data_warehouse.flow_sample_map[na_idx][switch_idx]);

        /*
        //destory the hashmaps
        ht_kfs_vi_destory(data_warehouse.flow_volume_map[na_idx][switch_idx]);
        ht_kfs_vi_fixSize_destory(data_warehouse.flow_sample_map[na_idx][switch_idx]);
        //recreate the hashmaps
        data_warehouse.flow_volume_map[na_idx][switch_idx] = ht_kfs_vi_create();
        if (data_warehouse.flow_volume_map[na_idx][switch_idx] == NULL) {
            return -1;
        }
        data_warehouse.flow_sample_map[na_idx][switch_idx] = ht_kfs_vi_fixSize_create();
        if (data_warehouse.flow_sample_map[na_idx][switch_idx] == NULL) {
            return -1;
        }
        */
    }
    return 0;
}

/**
* @brief data_warehouse_rotate_condition_buffer_idx(), data_warehouse_reset_noactive_buf()
* should use Mutex to control the access between two threads
* IntervalRotator thread and ConditionRatator thread;
* These two functions should be locked and unlocked simultaneously.
*/
void data_warehouse_rotate_condition_buffer_idx(void) {
    data_warehouse.active_condition_idx = (data_warehouse.active_condition_idx + 1) % BUFFER_NUM;
}
int data_warehouse_reset_condition_inactive_buf(void) {
    int na_condition_idx = (data_warehouse.active_condition_idx + 1) % BUFFER_NUM;
    int switch_idx = 0;
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        // as ovs may write condition packets to inactive buf, the hashmap cannot be destoryed
        // (The design is to make the ovs receive all condition pkts from senders, 
        //  then rotate the buffer. However, there is chance that there are condition pkts received after the rotation)
        //refreseh the inactive buffer
        ht_kfs_vi_refresh(data_warehouse.target_flow_map[na_condition_idx][switch_idx]);

        /*
        //destory the hashmap
        ht_kfs_vi_destory(data_warehouse.target_flow_map[na_condition_idx][switch_idx]);
        //recreate the hashmap
        data_warehouse.target_flow_map[na_condition_idx][switch_idx] = ht_kfs_vi_create();
        if (data_warehouse.target_flow_map[na_condition_idx][switch_idx] == NULL) {
            return -1;
        }
        */
    }
    return 0;
}

/**
* @brief 
*
* @param switch_id real_switch_id-1
*
* @return 
*/
hashtable_kfs_vi_t* data_warehouse_get_flow_volume_map(int switch_id) {
    return data_warehouse.flow_volume_map[data_warehouse.active_idx][switch_id];
}
hashtable_kfs_vi_t* data_warehouse_get_target_flow_map(int switch_id) {
    return data_warehouse.target_flow_map[data_warehouse.active_condition_idx][switch_id];
}
hashtable_kfs_vi_fixSize_t* data_warehouse_get_flow_sample_map(int switch_id) {
    return data_warehouse.flow_sample_map[data_warehouse.active_idx][switch_id];
}
hashtable_kfs_vi_t* data_warehouse_get_unactive_flow_volume_map(int switch_id){
    return data_warehouse.flow_volume_map[(data_warehouse.active_idx+1)%BUFFER_NUM][switch_id];
}
hashtable_kfs_vi_t* data_warehouse_get_unactive_target_flow_map(int switch_id) {
    return data_warehouse.target_flow_map[(data_warehouse.active_condition_idx+1)%BUFFER_NUM][switch_id];
}
hashtable_kfs_vi_fixSize_t* data_warehouse_get_unactive_sample_flow_map(int switch_id) {
    return data_warehouse.flow_sample_map[(data_warehouse.active_idx+1)%BUFFER_NUM][switch_id];
}
