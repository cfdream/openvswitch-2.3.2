#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"
#include "debug_output.h"

pthread_mutex_t cm_debug_file_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cm_notice_file_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cm_warn_file_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cm_error_file_mutex = PTHREAD_MUTEX_INITIALIZER;

void cm_request_mutex(pthread_mutex_t* p_mutex) {
    if (OPEN_LOCK ) {
        pthread_mutex_lock(p_mutex);
    }
}

void cm_release_mutex(pthread_mutex_t* p_mutex) {
    if (OPEN_LOCK) {
        pthread_mutex_unlock(p_mutex);
    }
}

void CM_DEBUG(int switch_id, const char* buffer) {
    FILE * fp;
    if (!OPEN_DEBUG) {
        return;
    }
    cm_request_mutex(&cm_debug_file_mutex);
    char fname[100];
    get_switch_output_file(switch_id, DEBUG_ID, fname, 100);
    fp = fopen(fname, "a+");
    if (fp == NULL) {
        printf("open file failed");
        cm_release_mutex(&cm_debug_file_mutex);
        return;
    }
    fputs(buffer, fp);
    fputc('\n', fp);
    fflush(fp);
    fclose(fp);
    cm_release_mutex(&cm_debug_file_mutex);
}

void CM_NOTICE(int switch_id, const char* buffer) {
    FILE * fp;
    if (!OPEN_NOTICE) {
        return;
    }
    cm_request_mutex(&cm_notice_file_mutex);
    char fname[100];
    get_switch_output_file(switch_id, NOTICE_ID, fname, 100);
    fp = fopen(fname, "a+");
    if (fp == NULL) {
        printf("open file failed");
        cm_release_mutex(&cm_notice_file_mutex);
        return;
    }
    fputs(buffer, fp);
    fputc('\n', fp);
    fflush(fp);
    fclose(fp);
    cm_release_mutex(&cm_notice_file_mutex);
}

void CM_WARNING(int switch_id, const char* buffer) {
    FILE * fp;
    if (!OPEN_WARNING) {
        return;
    }
    cm_request_mutex(&cm_warn_file_mutex);
    char fname[100];
    get_switch_output_file(switch_id, WARNING_ID, fname, 100);
    fp = fopen(fname, "a+");
    if (fp == NULL) {
        printf("open file failed");
        cm_release_mutex(&cm_warn_file_mutex);
        return;
    }
    fputs(buffer, fp);
    fputc('\n', fp);
    fflush(fp);
    fclose(fp);
    cm_release_mutex(&cm_warn_file_mutex);
}

void CM_ERROR(int switch_id, const char* buffer) {
    FILE * fp;
    if (!OPEN_ERROR) {
        return;
    }
    cm_request_mutex(&cm_error_file_mutex);
    char fname[100];
    get_switch_output_file(switch_id, ERROR_ID, fname, 100);
    fp = fopen(fname, "a+");
    if (fp == NULL) {
        printf("open file failed");
        cm_release_mutex(&cm_error_file_mutex);
        return;
    }
    fputs(buffer, fp);
    fputc('\n', fp);
    fflush(fp);
    fclose(fp);
    cm_release_mutex(&cm_error_file_mutex);
}

void get_switch_output_file(int switch_id, int level, char* fname, int fname_len) {
    if (level == DEBUG_ID) {
        snprintf(fname, fname_len, "%ss%d.debug", CM_RECEIVER_TARGET_FLOW_FNAME_PREFIX, switch_id);
    } else if (level == ERROR_ID) {
        snprintf(fname, fname_len, "%ss%d.error", CM_RECEIVER_TARGET_FLOW_FNAME_PREFIX, switch_id);
    } else if (level == WARNING_ID) {
        snprintf(fname, fname_len, "%ss%d.warning", CM_RECEIVER_TARGET_FLOW_FNAME_PREFIX, switch_id);
    } else if (level == NOTICE_ID) {
        snprintf(fname, fname_len, "%ss%d.notice", CM_RECEIVER_TARGET_FLOW_FNAME_PREFIX, switch_id);
    }
}


