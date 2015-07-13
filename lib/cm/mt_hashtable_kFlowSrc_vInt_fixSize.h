/* 
 * multi-thread hashtable 
 * key: <srcip> flow, or flow_src_t, or kfs-key flow_src
 * Value: uint
 * get()
 * set()
 * ht_kfs_vi_fixSize_next(): can only be used by one thread
 * del()
 *
 * */

#ifndef __CM_HASHTABLE_KFLOWSRC_VINT_FIXSIZE_H__
#define __CM_HASHTABLE_KFLOWSRC_VINT_FIXSIZE_H__

#define KEY_INT_TYPE uint32_t

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>
#include "../CM_testbed_code/public_lib/flow.h"
#include "../CM_testbed_code/public_lib/target_flow_setting.h"

struct entry_kfs_vi_fixSize_s {
	flow_src_t *key;
	KEY_INT_TYPE value;
};

typedef struct entry_kfs_vi_fixSize_s entry_kfs_vi_fixSize_t;

struct hashtable_kfs_vi_fixSize_s {
	int size;
	struct entry_kfs_vi_fixSize_s *table[SH_HASHMAP_SIZE];

    /* for multi-thread accessing */
    pthread_mutex_t mutexs[SH_HASHMAP_SIZE];
};

typedef struct hashtable_kfs_vi_fixSize_s hashtable_kfs_vi_fixSize_t;


/* 
* @brief Create a new hashtable.
*
* @param size
*
* @return 
*/
hashtable_kfs_vi_fixSize_t *ht_kfs_vi_fixSize_create(void);

void ht_kfs_vi_fixSize_destory( hashtable_kfs_vi_fixSize_t *hashtable );

/* Hash a string for a particular hash table. */
int ht_kfs_vi_fixSize_hash( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t *key );

/* Create a key-value pair. */
entry_kfs_vi_fixSize_t *ht_kfs_vi_fixSize_newpair( flow_src_t *key, KEY_INT_TYPE value );

/**
* @brief Retrieve a key-value pair from a hash table.
*
* @param hashtable
* @param key
*
* @return -1: key not exist in the hashtable, >=0 : value of the key
*/
int ht_kfs_vi_fixSize_get( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t* key );

/* Insert a key-value pair into a hash table. */
void ht_kfs_vi_fixSize_set( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t *key, KEY_INT_TYPE value );

/* del a key-value pair from a hash table. */
void ht_kfs_vi_fixSize_del( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t *key);

bool is_target_flow(flow_src_t* key);

#endif
