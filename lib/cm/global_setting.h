#ifndef GLOBAL_SETTING_H
#define GLOBAL_SETTING_H

#include "../CM_testbed_code/public_lib/target_flow_setting.h"

const uint64_t SECOND_2_USECOND = 1000000;
const uint64_t FIRST_INTERVAL_START_USECOND = 21600000000;
const uint64_t VOLUME_IN_30_SECONDS = 6181204282L;

//flag of condition information ends, switch will use to switch condition map
const int CONDITION_TERMINATE_FLOW_SRCIP = 0;
const int CONDITION_TERMINATE_ALL_VOLUME = 0;

//replace mechanism
const bool OPEN_REPLACE_MECHANISM = true;

//seconds in one interval
const int INTERVAL_SECONDS = 1;


//init every time
//second_2_usecond * interval_seconds;
uint64_t USECONDS_IN_ONE_INTERVAL = 0;
uint64_t VOLUME_IN_ONE_INTERVAL = 0;

int init_global_setting(void);
int init_global_setting(void) {
    USECONDS_IN_ONE_INTERVAL = INTERVAL_SECONDS * SECOND_2_USECOND;
    VOLUME_IN_ONE_INTERVAL = (uint64_t) (1.0 * VOLUME_IN_30_SECONDS / 30 * INTERVAL_SECONDS);

    return 0;
}

#endif
