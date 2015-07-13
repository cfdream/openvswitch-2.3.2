#include <config.h>
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "openvswitch/vlog.h"
#include "interval_rotator.h"

FILE* init_target_flow_file(void) {
    FILE* fp_target_flows;
    //1. get switch name
    /*need to test whether this works
    //test result: switch cannot be got in this way
    char hostname[100];
    if (get_mininet_host_name(hostname, 100) != 0) {
        printf("FAIL:get_mininet_host_name\n");
        return NULL;
    }
    */
    //TODO: should be tested whether work or not
    if (cm_switch_name == NULL) {
        printf("FAIL: cm_switch_name == NULL\n");
        return NULL; 
    }

    //2. generate target_flow_fname, sampled_flow_fname
    char target_flow_fname[200];
    snprintf(target_flow_fname, 200, "%s%s%s", 
        CM_RECEIVER_TARGET_FLOW_FNAME_PREFIX,
        cm_switch_name, CM_RECEIVER_TARGET_FLOW_FNAME_SUFFIX);
    //3. open target_flow_fname, sampled_flow_fname
    fp_target_flows = fopen(target_flow_fname, "a+");
    if (fp_target_flows == NULL) {
        printf("FAIL: fopen %s\n", target_flow_fname);
        return NULL;
    }

    fprintf(fp_target_flows, "%s\t%s\t%s\n", "srcip", "volume_travel_through", "volume_sampled");
    return fp_target_flows;
}

void write_target_flows_to_file(uint64_t current_sec, FILE* fp_target_flow) {
    fprintf(fp_target_flow, "time-%lu seconds\n", current_sec);
    hashtable_kfs_vi_t* target_flow_map_pre_interval = data_warehouse_get_unactive_target_flow_map();
    hashtable_kfs_vi_fixSize_t* sample_flow_map_pre_interval = data_warehouse_get_unactive_sample_flow_map();
    entry_kfs_vi_t ret_entry;
    while (ht_kfs_vi_next(target_flow_map_pre_interval, &ret_entry) == 0) {
        //get one target flow, output to file
        flow_s* p_flow = ret_entry.key;
        KEY_INT_TYPE all_volume = ret_entry.value;
        //get the sampled volume of the flow
        int sample_volume = ht_kfs_vi_fixSize_get(sample_flow_map_pre_interval, p_flow);
        if (sample_volume < 0) {
            sample_volume = 0;
        }
        fprintf(fp_target_flow, "%u\t%u\t%u\n", p_flow->srcip, all_volume, sample_volume);
    }
}

void* rotate_interval(void* param) {
    //sleep two second
    sleep(2);

    while (true) {
        /* all switches start/end at the nearby timestamp for intervals */
        /* postpone till switching to next time interval */
        uint64_t current_sec = get_next_interval_start(CM_TIME_INTERVAL);

        //1. rotate the data warehouse buffer
        data_ware_rotate_buffer();

        //2. store the target flow identities of the past interval into file
        FILE* fp_target_flow = init_target_flow_file();
        if (fp_target_flow == NULL) {
            printf("FAIL: init_target_flow_file\n");
            return NULL;
        }

        write_target_flows_to_file(current_sec, fp_target_flow);

        //close file
        fclose(fp_target_flow);

        //3. reset the idel buffer of data warehouse
        data_warehouse_reset_noactive_buf();
    }

    return NULL;
}
