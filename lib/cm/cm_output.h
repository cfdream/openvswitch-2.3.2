#ifndef __CM_DEBUG_OUTPUT_H__
#define __CM_DEBUG_OUTPUT_H__

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define DEBUG_ID 1
#define ERROR_ID 2
#define WARNING_ID 3
#define NOTICE_ID 4
#define OUTPUT_ID 5

#define OPEN_DEBUG 1
#define OPEN_NOTICE 1
#define OPEN_WARNING 1
#define OPEN_ERROR 1
#define OPEN_OUTPUT 1
#define OPEN_LOCK 0

/**
* @brief 
*
* @param switch_id real_switchid
* @param buffer
*/
void CM_DEBUG(int switch_id, const char* buffer);
void CM_NOTICE(int switch_id, const char* buffer);
void CM_WARNING(int switch_id, const char* buffer);
void CM_ERROR(int switch_id, const char* buffer);
void CM_OUTPUT(int switch_id, const char* buffer);
void CM_request_mutex(pthread_mutex_t* p_mutex);
void CM_release_mutex(pthread_mutex_t* p_mutex);
void get_switch_output_file(int switch_id, int level, char* fname, int fname_len);

#endif
