#ifndef __DATA_WAREHOUSE_H__
#define __DATA_WAREHOUSE_H__

#include "../CM_testbed_code/public_lib/mt_hashtable_kFlow_vLinklist.h"
#include "../CM_testbed_code/public_lib/mt_hashtable_kFlowSrc_vInt.h"
#include "../CM_testbed_code/public_lib/mt_hashtable_kFlowSrc_vFloat.h"
#include "mt_hashtable_kFlowSrc_vInt_fixSize.h"

#define BUFFER_NUM 2

/**
* @brief 
*/
typedef struct data_warehouse_s {
    /*Following are 2-buffer hashtables*/
    int active_idx;
    //4 hashtables for recording flow properties
    //here are <srcip> flow
    hashtable_kfs_vi_t* flow_volume_map[BUFFER_NUM];
    hashtable_kfs_vi_t* target_flow_map[BUFFER_NUM];

    /* 1 hashtable for sample and hold*/
    hashtable_kfs_vi_fixSize_t* flow_sample_map[BUFFER_NUM];
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
void data_ware_rotate_buffer(void);

/**
* @brief This should be called after data_ware_rotate_buffer() is called, and after non-active buffer is useless
*
*
* @return 0-succ, -1-fail
*/
int data_warehouse_reset_noactive_buf(void);

hashtable_kfs_vi_t* data_warehouse_get_flow_volume_map(void);

hashtable_kfs_vi_t* data_warehouse_get_target_flow_map(void);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_flow_sample_map(void);

hashtable_kfs_vi_t* data_warehouse_get_unactive_target_flow_map(void);

hashtable_kfs_vi_fixSize_t* data_warehouse_get_unactive_sample_flow_map(void);
#endif
