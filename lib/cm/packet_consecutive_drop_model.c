#include <config.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"
#include "packet_consecutive_drop_model.h"

extern cm_experiment_setting_t cm_experiment_setting;

uint64_t switches_ongoing_ms[NUM_SWITCHES]; //the ongoing millisecond
bool switches_ms_randomed[NUM_SWITCHES];
bool switches_ms_dropped[NUM_SWITCHES];

void init_packet_consecutive_drop_model(void) {
    memset(switches_ongoing_ms, 0, sizeof(switches_ongoing_ms));
    memset(switches_ms_randomed, 0, sizeof(switches_ms_randomed));
    memset(switches_ms_dropped, 0, sizeof(switches_ms_dropped));
}

bool drop_packet(int switch_id, struct drand48_data* p_rand_buffer) {
    struct timespec spec;
    double rand_float;

    clock_gettime(CLOCK_REALTIME, &spec);
    uint64_t ms = spec.tv_nsec / 1000000;

    if (switches_ongoing_ms[switch_id] != ms) {
        switches_ongoing_ms[switch_id] = ms;
        switches_ms_randomed[switch_id] = false;
        switches_ms_dropped[switch_id] = false;
    }

    if (!switches_ms_randomed[switch_id]) {
        switches_ms_randomed[switch_id] = true;
        //rand number
        drand48_r(p_rand_buffer, &rand_float);    //[0,1)
        if (rand_float < cm_experiment_setting.switch_drop_rate) {
            switches_ms_dropped[switch_id] = true;
        }
    }

    return switches_ms_dropped[switch_id];            
}
