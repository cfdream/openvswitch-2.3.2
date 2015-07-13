#ifndef GLOBAL_DATA_H
#define GLOBAL_DATA_H

#include "global_setting.h"

bool g_sampled_buffer_condition_buffer_initialized = false;
int g_current_interval;     //initialized in packet_processor.h when the first normal packet arrives         
bool g_first_packet = true;
bool g_first_normal_packet = true;
int g_received_pkt_num = 0;

//socket information
int server_port = 8888;
char server_ip[20] = "10.1.0.254";


//rand() mod number
const int RAND_MOD_NUMBER = 1000000;


#endif
