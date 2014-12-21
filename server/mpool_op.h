/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//mpool_op.h

#ifndef _MPOOL_OP_H
#define _MPOOL_OP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"
#include "hash.h"
#include "store.h"

#ifdef __cplusplus
extern "C" {
#endif

extern HashArray *g_hash_array;

int mp_init(StoreHandle **ppHandle, const u_int64_t nCacheSize);
int mp_destroy_instance(StoreHandle **ppHandle);
int mp_destroy();

int mp_memp_trickle(int *nwrotep);

int mp_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size);
int mp_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len);
int mp_partial_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int offset, const int value_len);
int mp_delete(StoreHandle *pHandle, const char *pKey, const int key_len);
int mp_inc(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len);

int mp_inc_ex(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int expires);

int mp_clear_expired_keys(void *arg);

#ifdef __cplusplus
}
#endif

#endif

