/* 
 * multi-thread hashtable 
 * key: flow_src_t, or kfs-key flow_src
 * Value: uint, one entry per bin
 * get()
 * set()
 * ht_kfs_vi_fixSize_next(): can only be used by one thread
 * del()
 *
 * */

#include <config.h>
#include "data_warehouse.h"
#include "mt_hashtable_kFlowSrc_vInt_fixSize.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"

/* 
* @brief Create a new hashtable.
*
* @param size
*
* @return 
*/
hashtable_kfs_vi_fixSize_t *ht_kfs_vi_fixSize_create(void) {

	hashtable_kfs_vi_fixSize_t *hashtable = NULL;
	int i;

	/* Allocate the table itself. */
	if( ( hashtable = malloc( sizeof( hashtable_kfs_vi_fixSize_t ) ) ) == NULL ) {
		return NULL;
	}

	for( i = 0; i < HASH_MAP_SIZE; i++ ) {
		hashtable->table[i] = NULL;
	}

    /* initialize mutexs */
    for (i = 0; i < HASH_MAP_SIZE; ++i) {
        pthread_mutex_init(&hashtable->mutexs[i], NULL);
    }

	hashtable->size = HASH_MAP_SIZE;

	return hashtable;	
}

void ht_kfs_vi_fixSize_destory( hashtable_kfs_vi_fixSize_t *hashtable ) {
    int i;
    entry_kfs_vi_fixSize_t* p_node;

    if (NULL == hashtable) {
        return;
    }
    //free table
    for (i = 0; i < hashtable->size; i++) {
        p_node = hashtable->table[i];
        if (p_node) {
            free(p_node->key);
            free(p_node);
        }
    }

    /* free mutexs */
    for (i = 0; i < HASH_MAP_SIZE; ++i) {
        pthread_mutex_destroy(&hashtable->mutexs[i]);
    }

    free(hashtable);
}

/* Hash a string for a particular hash table. */
int ht_kfs_vi_fixSize_hash( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t *key ) {
	/* generate a 64-bit integer from srcip and dstip */
	unsigned long long int hashval = key->srcip;
    hashval = ((hashval << 32) | key->dstip) ^ key->src_port ^ key->dst_port;

	return hashval % hashtable->size;
}

/* Create a key-value pair. */
entry_kfs_vi_fixSize_t *ht_kfs_vi_fixSize_newpair( flow_src_t *key, KEY_INT_TYPE value ) {
	entry_kfs_vi_fixSize_t *newpair;

	if( ( newpair = malloc( sizeof( entry_kfs_vi_fixSize_t ) ) ) == NULL ) {
		return NULL;
	}

    //copy the key and value
    newpair->key = deep_copy_flow(key);
    newpair->value = value;

	return newpair;
}

/**
* @brief Retrieve a key-value pair from a hash table.
*
* @param hashtable
* @param key
*
* @return -1: key not exist in the hashtable, >=0 : value of the key
*/
int ht_kfs_vi_fixSize_get( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t* key ) {
	int bin = 0;
	entry_kfs_vi_fixSize_t *pair;

    if (NULL == hashtable) {
        return -1;
    }

	bin = ht_kfs_vi_fixSize_hash( hashtable, key );

    /* request mutex */
    pthread_mutex_lock(&hashtable->mutexs[bin]);

	/* Step through the bin, looking for our value. */
	pair = hashtable->table[ bin ];
	/* Did we actually find anything? */
	if( pair == NULL || pair->key == NULL || flow_src_compare( key, pair->key ) != 0 ) {
        /* release mutex */
        pthread_mutex_unlock(&hashtable->mutexs[bin]);
		return -1;
	} else {
        /* release mutex */
        pthread_mutex_unlock(&hashtable->mutexs[bin]);
		return pair->value;
	}
}

/* Insert a key-value pair into a hash table. */
void ht_kfs_vi_fixSize_set(hashtable_kfs_vi_fixSize_t *hashtable, hashtable_kfs_vi_t* target_flow_map, flow_src_t *key, KEY_INT_TYPE value) {
	int bin = 0;
	entry_kfs_vi_fixSize_t *newpair = NULL;
	entry_kfs_vi_fixSize_t *next = NULL;

    if (NULL == hashtable) {
        return;
    }

	bin = ht_kfs_vi_fixSize_hash( hashtable, key );

    /* request mutex */
    pthread_mutex_lock(&hashtable->mutexs[bin]);

	next = hashtable->table[ bin ];

	if( next != NULL && next->key != NULL ) {
        /* There's already a pair. */
        if (flow_src_compare( key, next->key ) == 0 ) {
            //the flow exist
            next->value = value;
        } else {
            //another flow already exist
            //conflict happens
            if (CM_OPEN_REPLACE_MECHANISM
                && is_target_flow(target_flow_map, next->key)) {
                //replay && the existing flow is_target_flow, 
                /* keep the existing flow */
            } else {
                //replace with the new flow
                //1. free the existing pair
                free(next->key);
                free(next);
                //2. set the new pair
                newpair = ht_kfs_vi_fixSize_newpair( key, value );
                hashtable->table[ bin ] = newpair;
            }
        }
	/* The bin is empty */
	} else {
		newpair = ht_kfs_vi_fixSize_newpair( key, value );
        hashtable->table[ bin ] = newpair;
	}
    /* release mutex */
    pthread_mutex_unlock(&hashtable->mutexs[bin]);
}

/* del a key-value pair from a hash table. */
void ht_kfs_vi_fixSize_del( hashtable_kfs_vi_fixSize_t *hashtable, flow_src_t *key) {
	int bin = 0;
	entry_kfs_vi_fixSize_t *next = NULL;

    if (NULL == hashtable) {
        return;
    }

	bin = ht_kfs_vi_fixSize_hash( hashtable, key );

    /* request mutex */
    pthread_mutex_lock(&hashtable->mutexs[bin]);

	next = hashtable->table[ bin ];

	/* There's already a pair.  Let's del that entry. */
	if( next != NULL && next->key != NULL && flow_src_compare( key, next->key ) == 0 ) {
        free(next->key);
        free(next);
        hashtable->table[ bin ] = NULL;
	}

    /* release mutex */
    pthread_mutex_unlock(&hashtable->mutexs[bin]);
}

bool is_target_flow(hashtable_kfs_vi_t* target_flow_map, flow_src_t* key) {
    if (ht_kfs_vi_get(target_flow_map, key) < 0) {
        return false;
    } else {
        return true;
    }
}
