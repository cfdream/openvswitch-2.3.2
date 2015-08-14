#ifndef __DATA_WAREHOUSE_H__
#define __DATA_WAREHOUSE_H__

#include "../CM_testbed_code/public_lib/mt_hashtable_kFlow_vLinklist.h"
#include "../CM_testbed_code/public_lib/mt_hashtable_kFlowSrc_vInt.h"
#include "../CM_testbed_code/public_lib/mt_hashtable_kFlowSrc_vFloat.h"
#include "../CM_testbed_code/public_lib/cm_experiment_setting.h"
#include "mt_hashtable_kFlowSrc_vInt_fixSize.h"

#define BUFFER_NUM 2
#define DFAULT_CONDITION_MAP_SIZE 3333

/**
* @brief 
*/
typedef struct data_warehouse_s {
    /*Following are 2-buffer hashtables*/
    int active_idx;
    // flow ground truth volume map
    hashtable_kfs_vi_t* flow_volume_map[BUFFER_NUM][NUM_SWITCHES];
    // flow sampled volume map
    hashtable_kfs_vi_fixSize_t* flow_sample_map[BUFFER_NUM][NUM_SWITCHES];

    // target flow map
    int active_condition_idx;
    hashtable_kfs_vi_fixSize_t* target_flow_map[BUFFER_NUM][NUM_SWITCHES];
    /* for multi-thread accessing */
    pthread_mutex_t condition_map_mutex;

    /* interval infor: will not affect final results */
    // I select not to use mutex on the following instances,
    uint64_t pkt_num_rece[BUFFER_NUM][NUM_SWITCHES];
    uint64_t volume_rece[BUFFER_NUM][NUM_SWITCHES];
    uint64_t condition_pkt_num_rece[BUFFER_NUM][NUM_SWITCHES];
}data_warehouse_t;

data_warehouse_t data_warehouse;

/**
* @brief init the data warehouse, at the beginning, only the first buffer should be initiliazed
*
* @return 0-succ -1:fail
*/
int data_warehouse_init(void);

/**
* @brief call when need to switch to another buffer
*/
void data_warehouse_rotate_buffer_idx(void);
/**
* @brief This should be called after data_warehouse_rotate_buffer_idx() is called, and after non-active buffer is useless
*
*
* @return 0-succ, -1-fail
*/
int data_warehouse_reset_noactive_buf(void);

/**
* @brief data_warehouse_rotate_condition_buffer_idx(), data_warehouse_reset_noactive_buf()
* should use condition_map_mutex to control the access between two threads
* IntervalRotator thread and ConditionRatator thread;
* These two functions should be locked and unlocked simultaneously.
*/
void data_warehouse_rotate_condition_buffer_idx(void);
int data_warehouse_reset_condition_inactive_buf(void);

hashtable_kfs_vi_t* data_warehouse_get_flow_volume_map(int switch_id);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_target_flow_map(int switch_id);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_flow_sample_map(int switch_id);

hashtable_kfs_vi_t* data_warehouse_get_unactive_flow_volume_map(int switch_id);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_unactive_target_flow_map(int switch_id);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_unactive_sample_flow_map(int switch_id);

#endif
