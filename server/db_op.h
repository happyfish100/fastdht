/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//db_op.h

#ifndef _DB_OP_H
#define _DB_OP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include "store.h"
#include "fdht_define.h"
#include "fdht_types.h"

typedef DBTYPE DBType;

#ifdef __cplusplus
extern "C" {
#endif

int db_init(StoreHandle **ppHandle, const DBType type, \
	const u_int64_t nCacheSize, const u_int32_t page_size, \
	const char *base_path, const char *filename);
int db_destroy_instance(StoreHandle **ppHandle);
int db_destroy();

int db_sync(StoreHandle *pHandle);
int db_memp_trickle(int *nwrotep);
int db_memp_sync();

int db_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size);
int db_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len);
int db_partial_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int offset, const int value_len);
int db_delete(StoreHandle *pHandle, const char *pKey, const int key_len);
int db_inc(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len);

int db_inc_ex(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int timeout);

void *bdb_dl_detect_entrance(void *arg);

int db_clear_expired_keys(void *arg);

#ifdef __cplusplus
}
#endif

#endif

