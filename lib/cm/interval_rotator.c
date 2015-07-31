#include <config.h>
#include <pthread.h>
#include "openvswitch/vlog.h"
#include "cm_output.h"
#include "interval_rotator.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"

void init_target_flow_files(void) {
    char buffer[100];
    snprintf(buffer, 100, "%s\t%s\t%s", "srcip", "volume_travel_through", "volume_sampled");
    int switch_idx = 0;
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        CM_OUTPUT(switch_idx+1, buffer);
    }
}


void write_target_flows_to_file(uint64_t current_sec) {
    DEBUG("start: write_target_flows_to_file");
    int switch_idx = 0;
    char buf[100];
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        snprintf(buf, 100, "time-%lu seconds", current_sec);
        CM_OUTPUT(switch_idx+1, buf);
    }
    for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
        hashtable_kfs_vi_t* target_flow_map_pre_interval = data_warehouse_get_unactive_target_flow_map(switch_idx);
        hashtable_kfs_vi_t* flow_volume_map_pre_interval = data_warehouse_get_unactive_flow_volume_map(switch_idx);
        hashtable_kfs_vi_fixSize_t* sample_flow_map_pre_interval = data_warehouse_get_unactive_sample_flow_map(switch_idx);
        entry_kfs_vi_t ret_entry;
        while (ht_kfs_vi_next(target_flow_map_pre_interval, &ret_entry) == 0) {
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
        }
    }
    DEBUG("end: write_target_flows_to_file");
}

void* rotate_interval(void* param) {
    UNUSED(param);
    //sleep two second
    sleep(2);

    init_target_flow_files();

    while (true) {
        /* all switches start/end at the nearby timestamp for intervals */
        /* postpone till switching to next time interval */
        uint64_t current_sec = get_next_interval_start(CM_TIME_INTERVAL);

        pthread_mutex_lock(&data_warehouse.target_flow_map_mutex);

        /* output time */
        char time_str[100];
        snprintf(time_str, 100, "start: rotate_interval, current time:%lu", current_sec);
        DEBUG(time_str);

        //1. rotate normal buffer idx
        data_warehouse_rotate_buffer_idx();
        //1.1 rotate the condition buffer idx
        data_warehouse_rotate_condition_buffer_idx();

        //2. store the target flow identities of the past interval into file
        write_target_flows_to_file(current_sec);

        //3. reset the idel buffer of data warehouse
        data_warehouse_reset_noactive_buf();
        //3.1 reset the idle condition buffer
        data_warehouse_reset_condition_inactive_buf();

        pthread_mutex_unlock(&data_warehouse.target_flow_map_mutex);
        DEBUG("end: rotate_interval");
    }

    return NULL;
}
