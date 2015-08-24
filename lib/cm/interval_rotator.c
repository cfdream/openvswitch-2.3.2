#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "openvswitch/vlog.h"
#include "cm_output.h"
#include "interval_rotator.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"

extern cm_experiment_setting_t cm_experiment_setting;

void init_target_flow_files(void) {
    char buffer[100];
    snprintf(buffer, 100, "%s\t%s\t%s", "srcip", "volume_travel_through", "volume_sampled");
    int switch_idx = 0;
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        CM_OUTPUT(switch_idx+1, buffer);
    }
}

void write_interval_info_to_file(uint64_t current_msec) {
    char buf[100];
    int switch_idx = 0;
    int na_idx = (data_warehouse.active_idx+1)%BUFFER_NUM;
    for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
        CM_DEBUG(switch_idx+1, "start: write_target_flows_to_file");
        snprintf(buf, 100, "=====time-%lu milliseconds=====", current_msec);
        CM_OUTPUT(switch_idx+1, buf);

        hashtable_kfs_vi_fixSize_t* sample_flow_map_pre_interval = data_warehouse_get_unactive_sample_flow_map(switch_idx);
        hashtable_kfs_vi_fixSize_t* target_flow_map_pre_interval = data_warehouse_get_unactive_target_flow_map(switch_idx);
        snprintf(buf, 100, "sample_hashmap_size:%u", sample_flow_map_pre_interval->size);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "condition_hashmap_size:%u", target_flow_map_pre_interval->size);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "sample_hashmap collision times:%u", sample_flow_map_pre_interval->collision_times);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "condition_hashmap collision times:%lu", data_warehouse.condition_map_collision_times[na_idx][switch_idx]);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "condition_hashmap last rotate collision times:%lu", data_warehouse.condition_map_last_rotate_collision_times[na_idx][switch_idx]);
        CM_OUTPUT(switch_idx+1, buf);
        
        snprintf(buf, 100, "pkt_num_rece:%lu", data_warehouse.pkt_num_rece[na_idx][switch_idx]);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "volume_rece:%lu", data_warehouse.volume_rece[na_idx][switch_idx]);
        CM_OUTPUT(switch_idx+1, buf);
        snprintf(buf, 100, "condition_pkt_num_rece:%lu", data_warehouse.condition_pkt_num_rece[na_idx][switch_idx]);
        CM_OUTPUT(switch_idx+1, buf);
    }
}

void write_target_flows_to_file(void) {
    int switch_idx = 0;
    char buf[100];
    for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
        hashtable_kfs_vi_t* flow_volume_map_pre_interval = data_warehouse_get_unactive_flow_volume_map(switch_idx);
        hashtable_kfs_vi_fixSize_t* sample_flow_map_pre_interval = data_warehouse_get_unactive_sample_flow_map(switch_idx);
        hashtable_kfs_vi_fixSize_t* target_flow_map_pre_interval = data_warehouse_get_unactive_target_flow_map(switch_idx);
        //output every flow travelling through this switch
        entry_kfs_vi_t ret_entry;
        while (ht_kfs_vi_next(flow_volume_map_pre_interval, &ret_entry) == 0) {
            flow_s* p_flow = ret_entry.key;
            int all_volume = ret_entry.value;
            //get the sampled volume of the flow
            int sample_volume = ht_kfs_vi_fixSize_get(sample_flow_map_pre_interval, p_flow);
            //get whether this flow is marked as the target flow from received condition information
            int is_signed_as_target_flow = ht_kfs_vi_fixSize_get(target_flow_map_pre_interval, p_flow);
            //-1: not exist in the hashmap
            snprintf(buf, 100, "%u\t%d\t%d\t%d", p_flow->srcip, all_volume, sample_volume, is_signed_as_target_flow);
            CM_OUTPUT(switch_idx+1, buf);

            //free ret_entry.key
            free(ret_entry.key);
        }
        /*
        entry_kfs_vi_fixSize_t ret_entry;
        while (ht_kfs_vi_fixSize_next(target_flow_map_pre_interval, &ret_entry) == 0) {
            //get one target flow at the switch, output to file
            flow_s* p_flow = ret_entry.key;

            //get the ground truth volume of the flow
            int all_volume = ht_kfs_vi_get(flow_volume_map_pre_interval, p_flow);
            if (all_volume < 0) {
                //the target flow has not travelled through the switch
                //no need to write this flow to file
                continue;
            }

            //get the sampled volume of the flow
            int sample_volume = ht_kfs_vi_fixSize_get(sample_flow_map_pre_interval, p_flow);
            if (sample_volume < 0) {
                sample_volume = 0;
            }
            snprintf(buf, 100, "%u\t%u\t%u", p_flow->srcip, all_volume, sample_volume);
            CM_OUTPUT(switch_idx+1, buf);
            free(ret_entry.key);
        }
        */
    }
    for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
        CM_DEBUG(switch_idx+1, "end: write_target_flows_to_file");
    }
}

void* rotate_interval(void* param) {
    UNUSED(param);
    //sleep two second
    sleep(2);

    init_target_flow_files();
    struct timespec spec;
    uint64_t sec;
    int switch_idx = 0;

    while (true) {
        /* all switches start/end at the nearby timestamp for intervals */
        /* postpone till switching to next time interval */
        uint64_t current_msec = get_next_interval_start(cm_experiment_setting.interval_msec_len);

        //0. get a copy of pre interval target_flow_map
        //As the write process will take time,
        //and condition_rotator thread will needs to take the mutex
        pthread_mutex_lock(&data_warehouse.condition_map_mutex);

        /* output time */
        char time_str[100];
        snprintf(time_str, 100, "=====START: rotate_interval, current time:%lu ms=====", current_msec);
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            CM_DEBUG(switch_idx+1, time_str);
        }

        //1. rotate normal buffer idx
        data_warehouse_rotate_buffer_idx();
        //1.1 rotate the condition buffer idx
        data_warehouse_rotate_condition_buffer_idx();
        //hashtable_kfs_vi_fixSize_t* copy_target_flow_map_pre_interval = ht_kfs_vi_fixSize_copy(target_flow_map_pre_interval);

        //2. store the target flow identities of the past interval into file
        write_interval_info_to_file(current_msec);
        write_target_flows_to_file();

        //3. reset the idel buffer of data warehouse
        data_warehouse_reset_noactive_buf();
        //3.1 reset the idle condition buffer
        data_warehouse_reset_condition_inactive_buf();

        clock_gettime(CLOCK_REALTIME, &spec);
        sec = (intmax_t)((time_t)spec.tv_sec);
        snprintf(time_str, 100, "=====END: rotate_interval, current time:%lu=====", sec);
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            CM_DEBUG(switch_idx+1, time_str);
        }
        //destory the copied hashtable
        //ht_kfs_vi_fixSize_destory(copy_target_flow_map_pre_interval);
        pthread_mutex_unlock(&data_warehouse.condition_map_mutex);
    }
    return NULL;
}
