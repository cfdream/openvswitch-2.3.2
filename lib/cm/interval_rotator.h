#ifndef __INTERVAL_ROTATOR_H__
#define __INTERVAL_ROTATOR_H__

#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include "data_warehouse.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"
#include "../../CM_testbed_code/public_lib/senderFIFOsManager.h"
#include "../../CM_testbed_code/public_lib/mt_hashtable_kFlow_vInt.h"

FILE* init_target_flow_file(void);

void write_target_flows_to_file(uint64_t current_sec, FILE* fp_target_flow);

void* rotate_interval(void*);

#endif