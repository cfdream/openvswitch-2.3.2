#ifndef FIXED_SIZE_HASH_MAP_H
#define FIXED_SIZE_HASH_MAP_H

#include <pthread.h>
#include "measure_data_type.h"
#include "common_lib.h"
#include "condition_map.h"

/* sampled hashtable*/
#define SAMPLE_FIXED_SIZE 1200007 //1.2 Mbytes
#define SAMPLE_LOCK_SIZE 1000     //lock size

typedef struct sampled_flows_map_s {
    sampled_record_t sampled_flows[SAMPLE_FIXED_SIZE];
    pthread_mutex_t mutexs[SAMPLE_LOCK_SIZE];

}sampled_flow_map_t;

typedef struct sampled_buffers_s {
    int idx;
    sampled_flow_map_t sampled_flow_map[2];
} sampled_buffers_t;

sampled_buffers_t sampled_buffers;

//switch the sampled_buffers
void switch_buffer(int ith_interval);
//initial sampled_map
void initial_sampled_map(int idx);
//get the index of the lock
int get_lock_index_sampled_map(flow_key_t flow_key);
//get the lock
void request_sampled_map_lock(flow_key_t flow_key, sampled_flow_map_t* p_sampled_flow_map);
//release the lock
void release_sampled_map_lock(flow_key_t flow_key, sampled_flow_map_t* p_sampled_flow_map);
//get the index of sampled flow
int get_sampled_flow_index(flow_key_t flow_key);
//check whether flow_key exists 
bool flow_key_exist(flow_key_t flow_key);
//get the volume of one flow
uint64_t get_sampled_all_ports_volume(flow_key_t flow_key);
//put the flow sampled volume
void add_sampled_volume(flow_key_t flow_key, int32_t outport, uint64_t volume);
//clear sampled_record_t
void clear_sampled_record(sampled_record_t* p_sampled_record);

//initial sampled_map
void initial_sampled_map(int idx) {
    int i;
    sampled_flow_map_t* p_sampled_flow_map = &sampled_buffers.sampled_flow_map[idx];
    memset(p_sampled_flow_map->sampled_flows, 0, sizeof(p_sampled_flow_map->sampled_flows));
    for ( i = 0; i < SAMPLE_LOCK_SIZE; i++) {
        pthread_mutex_init(&p_sampled_flow_map->mutexs[i], NULL);
    }
}
//get the index of the lock
int get_lock_index_sampled_map(flow_key_t flow_key) {
    return flow_key.srcip % SAMPLE_FIXED_SIZE;
}
//get the lock
void request_sampled_map_lock(flow_key_t flow_key, sampled_flow_map_t* p_sampled_flow_map) {
    int idx = get_lock_index_sampled_map(flow_key);
    request_mutex(&p_sampled_flow_map->mutexs[idx]);
}
//release the lock
void release_sampled_map_lock(flow_key_t flow_key, sampled_flow_map_t* p_sampled_flow_map) {
    int idx = get_lock_index_sampled_map(flow_key);
    release_mutex(&p_sampled_flow_map->mutexs[idx]);
}

//get the index of sampled flow
int get_sampled_flow_index(flow_key_t flow_key) {
    return flow_key.srcip % SAMPLE_FIXED_SIZE;
}

//check whether flow_key exists 
bool flow_key_exist(flow_key_t flow_key){
    int idx = 0;
    sampled_flow_map_t* p_sampled_flow_map = &sampled_buffers.sampled_flow_map[sampled_buffers.idx];

    //lock
    request_sampled_map_lock(flow_key, p_sampled_flow_map);

    idx = get_sampled_flow_index(flow_key);
    if (p_sampled_flow_map->sampled_flows[idx].flow_key.srcip == flow_key.srcip) {
        //unlock
        release_sampled_map_lock(flow_key, p_sampled_flow_map);
        return true;
    }
    //unlock
    release_sampled_map_lock(flow_key, p_sampled_flow_map);
    return false;
}

//get the volume of one flow to every outport
uint64_t get_sampled_all_ports_volume(flow_key_t flow_key) {
    int idx = 0;
    int i = 0;
    uint64_t volume = 0;

    sampled_flow_map_t* p_sampled_flow_map = &sampled_buffers.sampled_flow_map[sampled_buffers.idx];
    //lock
    request_sampled_map_lock(flow_key, p_sampled_flow_map);
    //get data
    idx = get_sampled_flow_index(flow_key);
    if (p_sampled_flow_map->sampled_flows[idx].flow_key.srcip == flow_key.srcip) {
        //gather all volume
        for (; i < PORT_SIZE; i++) {
            volume += p_sampled_flow_map->sampled_flows[idx].port_volumes[i];
        }
        //volume = p_sampled_flow_map->sampled_flows[idx].volume;
    }
    //unlock
    release_sampled_map_lock(flow_key, p_sampled_flow_map);
    return volume;
}
//put the flow sampled volume
void add_sampled_volume(flow_key_t flow_key, int32_t outport, uint64_t volume) {
    int idx = 0;
    double loss_rate;
    bool keep_existing_flow = false;    //true: skip the flow_key
    sampled_record_t existing_flow;
    //get the pointer of sampled_flow_map
    sampled_flow_map_t* p_sampled_flow_map = &sampled_buffers.sampled_flow_map[sampled_buffers.idx];
    //lock
    request_sampled_map_lock(flow_key, p_sampled_flow_map);
    //put data
    idx = get_sampled_flow_index(flow_key);
    existing_flow = p_sampled_flow_map->sampled_flows[idx];
    if (existing_flow.flow_key.srcip == 0) {
        //the bucket is empty
        keep_existing_flow = false;
    } else {
        //the bucket is occupied
        if(existing_flow.flow_key.srcip == flow_key.srcip)
        {
            //the same flow is in the bucket
            //add the volume
            keep_existing_flow = true;
        } else {
            //different flow is in the bucket
            WARNING("p_sampled_flow_map confilt happens");
            if (!OPEN_REPLACE_MECHANISM) {
                //no replace mechanism, write directly
                keep_existing_flow = false;
            } else {
                //open replace mechanism
                //get the loss rate of existing flow;
                loss_rate = get_condition(flow_key).loss_rate;
                if (loss_rate >= TARGET_LOSS_RATE_THRESHOLD ) {
                    //loss rate of existing flow meets the threshold, keep the existing flow
                    //add the volume
                    keep_existing_flow = true;
                } else {
                    //loss rate of existing flow < threshold, overwrite the existing flow_key
                    keep_existing_flow = false;
                }
            } // else
        } // else
    } // else
    if (keep_existing_flow) {
        p_sampled_flow_map->sampled_flows[idx].port_volumes[outport] += volume;
    } else {
        clear_sampled_record(&p_sampled_flow_map->sampled_flows[idx]);
        p_sampled_flow_map->sampled_flows[idx].flow_key = flow_key;
        p_sampled_flow_map->sampled_flows[idx].port_volumes[outport] = volume;
    }
    // unlock
    release_sampled_map_lock(flow_key, p_sampled_flow_map);
}

//clear sampled_record 
void clear_sampled_record(sampled_record_t* p_sampled_record) {
    memset(p_sampled_record, 0, sizeof(sampled_record_t));
}
#endif
