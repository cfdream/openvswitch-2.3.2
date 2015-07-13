#ifndef CONDITION_MAP_H
#define CONDITION_MAP_H


#include <pthread.h>
#include "measure_data_type.h"
#include "debug_output.h"
#include "common_lib.h"

//TODO: find some prime number here
#define CONDITION_FIXED_SIZE 12000007 //12 Mbytes
#define CONDITION_LOCK_SIZE 1000     //lock size

typedef struct condition_flows_map_s {
    condition_record_t condition_flows[CONDITION_FIXED_SIZE];
    pthread_mutex_t mutexs[CONDITION_LOCK_SIZE];

}condition_flow_map_t;

typedef struct condition_buffers_s {
    int idx;
    condition_flow_map_t condition_flow_map[2];
} condition_buffers_t;

condition_buffers_t condition_buffers;

/*function defintion*/
//initial the mutexs in condition_flow_map
void initial_condition_flow_map(int idx);
//get index of lock
int get_lock_index_condition_flow_map(flow_key_t flow_key);
//get lock
void request_condition_flow_map_lock(flow_key_t flow_key, condition_flow_map_t* p_condition_flow_map);
//release lock
void release_condition_flow_map_lock(flow_key_t flow_key, condition_flow_map_t* p_condition_flow_map);
//get the condition_flow index of one flow
int get_index_in_condition_flow_map(flow_key_t flow_key);
//get condition rate of one flow 
condition_t get_condition(flow_key_t flow_key);
//get condition of one flow from the rest buffer
condition_t get_condition_from_rest_buffer(flow_key_t flow_key);
//put condition
void put_condition(flow_key_t flow_key, condition_t condition);

void switch_condition_buffer();

//initial the mutexs in condition_flow_map
void initial_condition_flow_map(int idx) {
    int i = 0;
    condition_flow_map_t* p_condition_flow_map = &condition_buffers.condition_flow_map[idx];
    memset(p_condition_flow_map->condition_flows, 0, sizeof(p_condition_flow_map->condition_flows));
    for (; i < CONDITION_LOCK_SIZE; i++) {
        pthread_mutex_init(&p_condition_flow_map->mutexs[i], NULL);
    }
}

/*function declarision*/
//get index of the lock
int get_lock_index_condition_flow_map(flow_key_t flow_key) {
    return flow_key.srcip % CONDITION_LOCK_SIZE;
}
//get lock
void request_condition_flow_map_lock(flow_key_t flow_key, condition_flow_map_t* p_condition_flow_map) {
    int idx = get_lock_index_condition_flow_map(flow_key);
    request_mutex(&p_condition_flow_map->mutexs[idx]);
}
//release lock
void release_condition_flow_map_lock(flow_key_t flow_key, condition_flow_map_t* p_condition_flow_map) {
    int idx = get_lock_index_condition_flow_map(flow_key);
    release_mutex(&p_condition_flow_map->mutexs[idx]);
}
//get the condition_flow index of one flow
int get_index_in_condition_flow_map(flow_key_t flow_key) {
    return flow_key.srcip % CONDITION_FIXED_SIZE;
}
//get condition of one flow 
condition_t get_condition(flow_key_t flow_key) {
    //if no condition_record data, return zero
    int idx;
    condition_flow_map_t* p_condition_flow_map = NULL;
    condition_t condition;
    condition.loss_rate = 0;
    condition.volume = 0;
    p_condition_flow_map = &condition_buffers.condition_flow_map[condition_buffers.idx];
    //lock
    request_condition_flow_map_lock(flow_key, p_condition_flow_map);
    //get data
    idx = get_index_in_condition_flow_map(flow_key);
    if (p_condition_flow_map->condition_flows[idx].flow_key.srcip == flow_key.srcip) {
        //the flow exists 
        condition = p_condition_flow_map->condition_flows[idx].condition;
    }
    //unlock
    release_condition_flow_map_lock(flow_key, p_condition_flow_map);
    return condition;
}
//put condition of one flow
void put_condition(flow_key_t flow_key, condition_t condition) {
    int flow_idx = 0;
    condition_flow_map_t* p_condition_flow_map = &condition_buffers.condition_flow_map[condition_buffers.idx];
    //lock
    request_condition_flow_map_lock(flow_key, p_condition_flow_map);
    //put data
    flow_idx = get_index_in_condition_flow_map(flow_key);
    if (p_condition_flow_map->condition_flows[flow_idx].flow_key.srcip
        && p_condition_flow_map->condition_flows[flow_idx].flow_key.srcip != flow_key.srcip) {
        WARNING("condition_flow_map confilt happens");
    }
    p_condition_flow_map->condition_flows[flow_idx].flow_key = flow_key;
    p_condition_flow_map->condition_flows[flow_idx].condition = condition;
    //unlock
    release_condition_flow_map_lock(flow_key, p_condition_flow_map);
}

//put condition of one flow
void put_condition_in_rest_buffer(flow_key_t flow_key, condition_t condition) {
    int flow_idx = 0;
    condition_flow_map_t* p_condition_flow_map = &condition_buffers.condition_flow_map[1-condition_buffers.idx];
    //lock
    request_condition_flow_map_lock(flow_key, p_condition_flow_map);
    //put data
    flow_idx = get_index_in_condition_flow_map(flow_key);
    if (p_condition_flow_map->condition_flows[flow_idx].flow_key.srcip
        && p_condition_flow_map->condition_flows[flow_idx].flow_key.srcip != flow_key.srcip) {
        WARNING("condition_flow_map confilt happens");
    }
    p_condition_flow_map->condition_flows[flow_idx].flow_key = flow_key;
    p_condition_flow_map->condition_flows[flow_idx].condition = condition;
    //unlock
    release_condition_flow_map_lock(flow_key, p_condition_flow_map);
}

//get condition of one flow from the rest buffer
//no lock is needed, as only the controller_communicator would use it;
condition_t get_condition_from_rest_buffer(flow_key_t flow_key) {
    //if no loss rate data, return zero condition
    int idx;
    condition_flow_map_t* p_condition_flow_map;
    condition_t condition;
    condition.loss_rate = 0;
    condition.volume = 0;
    p_condition_flow_map = &condition_buffers.condition_flow_map[1-condition_buffers.idx];
    //get data
    idx = get_index_in_condition_flow_map(flow_key);
    if (p_condition_flow_map->condition_flows[idx].flow_key.srcip == flow_key.srcip) {
        //the flow exists 
        condition = p_condition_flow_map->condition_flows[idx].condition;
    }
    return condition;
}
void switch_condition_buffer() {
    condition_buffers.idx = 1 - condition_buffers.idx;
}
#endif
