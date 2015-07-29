#include <config.h>
#include "openvswitch/vlog.h"
#include "cm_output.h"
#include "interval_rotator.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"

void init_target_flow_files(void) {
    char buffer[100];
    snprintf(buffer, 100, "%s\t%s\t%s", "srcip", "volume_travel_through", "volume_sampled");
    int switch_idx = 0;
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        CM_OUTPUT(switch_idx+1, buffer);
    }
}

void write_target_flows_to_file(uint64_t current_sec) {
    int switch_idx = 0;
    char buf[100];
    for (; switch_idx < NUM_SWITCHES; ++switch_idx) {
        snprintf(buf, 100, "time-%lu seconds", current_sec);
        CM_OUTPUT(switch_idx+1, buf);
    }
    for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
        hashtable_kfs_vi_t* target_flow_map_pre_interval = data_warehouse_get_unactive_target_flow_map(switch_idx);
        hashtable_kfs_vi_fixSize_t* sample_flow_map_pre_interval = data_warehouse_get_unactive_sample_flow_map(switch_idx);
        entry_kfs_vi_t ret_entry;
        while (ht_kfs_vi_next(target_flow_map_pre_interval, &ret_entry) == 0) {
            //get one target flow at the switch, output to file
            flow_s* p_flow = ret_entry.key;
            KEY_INT_TYPE all_volume = ret_entry.value;
            //get the sampled volume of the flow
            int sample_volume = ht_kfs_vi_fixSize_get(sample_flow_map_pre_interval, p_flow);
            if (sample_volume < 0) {
                sample_volume = 0;
            }
            snprintf(buf, 100, "%u\t%u\t%u", p_flow->srcip, all_volume, sample_volume);
            CM_OUTPUT(switch_idx+1, buf);
        }
    }
}

void* rotate_interval(void* param) {
    //sleep two second
    sleep(2);

    init_target_flow_files();

    while (true) {
        /* all switches start/end at the nearby timestamp for intervals */
        /* postpone till switching to next time interval */
        uint64_t current_sec = get_next_interval_start(CM_TIME_INTERVAL);
        /* output time */
        char time_str[100];
        snprintf(time_str, 100, "current time:%lu\n", current_sec);
        NOTICE(time_str);

        //1. rotate the data warehouse buffer
        data_ware_rotate_buffer();

        //2. store the target flow identities of the past interval into file
        write_target_flows_to_file(current_sec);

        //3. reset the idel buffer of data warehouse
        data_warehouse_reset_noactive_buf();
    }

    return NULL;
}
