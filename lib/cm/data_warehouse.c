#include <config.h>
#include "data_warehouse.h"
#include "../../CM_testbed_code/public_lib/time_library.h"

/**
* @brief init the data warehouse, at the beginning, only the first buffer should be initiliazed
*
* @return 0-succ -1:fail
*/
int data_warehouse_init(void) {
    //set the initial buffer
    int a_idx = 0;

    for (; a_idx < BUFFER_NUM; ++a_idx) {
        data_warehouse.flow_volume_map[a_idx] = ht_kfs_vi_create();
        if (data_warehouse.flow_volume_map[a_idx] == NULL) {
            return -1;
        }
        data_warehouse.target_flow_map[a_idx] = ht_kfs_vi_create();
        if (data_warehouse.target_flow_map[a_idx] == NULL) {
            return -1;
        }
        data_warehouse.flow_sample_map[a_idx] = ht_kfs_vi_fixSize_create();
        if (data_warehouse.flow_sample_map[a_idx] == NULL) {
            return -1;
        }
    }

    data_warehouse.active_idx = 0;
    NOTICE("SUCC data_warehouse_init");

    return 0;
}



/**
* @brief call when need to switch to another buffer
*/
void data_ware_rotate_buffer(void) {
    data_warehouse.active_idx = (data_warehouse.active_idx + 1) % BUFFER_NUM;
}

/**
* @brief This should be called after data_ware_rotate_buffer() is called, and after non-active buffer is useless
*
*
* @return 0-succ, -1-fail
*/
int data_warehouse_reset_noactive_buf(void) {
    //noactive buffer idx
    int na_idx = (data_warehouse.active_idx + 1) % BUFFER_NUM;
    //destory the hashmaps
    ht_kfs_vi_destory(data_warehouse.flow_volume_map[na_idx]);
    ht_kfs_vi_destory(data_warehouse.target_flow_map[na_idx]);
    ht_kfs_vi_fixSize_destory(data_warehouse.flow_sample_map[na_idx]);
    //recreate the hashmaps
    data_warehouse.flow_volume_map[na_idx] = ht_kfs_vi_create();
    if (data_warehouse.flow_volume_map[na_idx] == NULL) {
        return -1;
    }
    data_warehouse.target_flow_map[na_idx] = ht_kfs_vi_create();
    if (data_warehouse.target_flow_map[na_idx] == NULL) {
        return -1;
    }
    data_warehouse.flow_sample_map[na_idx] = ht_kfs_vi_fixSize_create();
    if (data_warehouse.flow_sample_map[na_idx] == NULL) {
        return -1;
    }
    return 0;
}

hashtable_kfs_vi_t* data_warehouse_get_flow_volume_map(void) {
    return data_warehouse.flow_volume_map[data_warehouse.active_idx];
}

hashtable_kfs_vi_t* data_warehouse_get_target_flow_map(void) {
    return data_warehouse.target_flow_map[data_warehouse.active_idx];
}

hashtable_kfs_vi_fixSize_t* data_warehouse_get_flow_sample_map(void) {
    return data_warehouse.flow_sample_map[data_warehouse.active_idx];
}

hashtable_kfs_vi_t* data_warehouse_get_unactive_target_flow_map(void) {
    return data_warehouse.target_flow_map[(data_warehouse.active_idx+1)%BUFFER_NUM];
}

hashtable_kfs_vi_fixSize_t* data_warehouse_get_unactive_sample_flow_map(void) {
    return data_warehouse.flow_sample_map[(data_warehouse.active_idx+1)%BUFFER_NUM];
}