#include <unistd.h>
#include <config.h>
#include <pthread.h>
#include "condition_rotator.h"
#include "data_warehouse.h"
#include "cm_output.h"
#include "../../CM_testbed_code/public_lib/time_library.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"

extern cm_experiment_setting_t cm_experiment_setting;

void* rotate_condition_buffers(void* param) {
    UNUSED(param);
    int switch_idx = 0;
    struct timespec spec;
    while (1) {
        /* postpone till the next timestamp that condition buffer needs to switch */
        uint64_t current_msec = get_next_interval_start(cm_experiment_setting.condition_msec_freq);
        /* postpone CM_CONDITION_MILLISECONDS_POSTPOINE_FOR_SWITCH
         * This is to make sure that the switches can receive all the condition information
         * This should be too large to make sure the condition infor is used at switches in time.
         * */
        //1second = 10^6 microsecond
        //sleep(1000000* CM_CONDITION_MILLISECONDS_POSTPOINE_FOR_SWITCH);
        //1millisecond = 10^3 microsecond(us)
        usleep(1000*CM_CONDITION_MILLISECONDS_POSTPOINE_FOR_SWITCH);

        pthread_mutex_lock(&data_warehouse.condition_map_mutex);

        /* output time */
        char time_str[100];
        snprintf(time_str, 100, "-----Start: rotate_condition_buffers, current time:%lu ms-----", current_msec);
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            CM_DEBUG(switch_idx+1, time_str);
        }

        //1 rotate the condition buffer idx
        data_warehouse_rotate_condition_buffer_idx();

        //2 reset the idle condition buffer
        data_warehouse_reset_condition_inactive_buf();

        clock_gettime(CLOCK_REALTIME, &spec);
        uint64_t msec = (intmax_t)((time_t)spec.tv_sec*1000 + spec.tv_nsec/1000000);
        snprintf(time_str, 100, "-----End: rotate_condition_buffers, current time:%lu ms-----", msec);
        for (switch_idx = 0; switch_idx < NUM_SWITCHES; ++switch_idx) {
            CM_DEBUG(switch_idx+1, time_str);
        }

        pthread_mutex_unlock(&data_warehouse.condition_map_mutex);
    }
}
