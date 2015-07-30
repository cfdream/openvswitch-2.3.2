#include <unistd.h>
#include <config.h>
#include <pthread.h>
#include "condition_rotator.h"
#include "data_warehouse.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"

void* rotate_condition_buffers(void* param) {
    UNUSED(param);
    while (1) {
        /* postpone till the next timestamp that condition buffer needs to switch */
        uint64_t current_sec = get_next_interval_start(CM_CONDITION_TIME_INTERVAL);
        /* postpone CM_CONDITION_TIME_INTERVAL_POSTPOINE_FOR_SWITCH
         * This is to make sure that the switches can receive all the condition information
         * This should be too large to make sure the condition infor is used at switches in time.
         * */
        sleep(CM_CONDITION_TIME_INTERVAL_POSTPOINE_FOR_SWITCH);

        /* output time */
        char time_str[100];
        snprintf(time_str, 100, "rotate_condition_interval, current time:%lu\n", current_sec);
        NOTICE(time_str);

        pthread_mutex_lock(&data_warehouse.target_flow_map_mutex);


        //1 rotate the condition buffer idx
        data_warehouse_rotate_condition_buffer_idx();

        //2 reset the idle condition buffer
        data_warehouse_reset_condition_inactive_buf();

        pthread_mutex_unlock(&data_warehouse.target_flow_map_mutex);
    }
}
