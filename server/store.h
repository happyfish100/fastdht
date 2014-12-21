/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//store.h

#ifndef _STORE_H
#define _STORE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"

typedef void StoreHandle;

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*func_destroy_instance)(StoreHandle **ppHandle);
typedef int (*func_destroy)();

typedef int (*func_memp_trickle)(int *nwrotep);

typedef int (*func_get)(StoreHandle *pHandle, const char *pKey, \
		const int key_len, char **ppValue, int *size);
typedef int (*func_set)(StoreHandle *pHandle, const char *pKey, \
		const int key_len, const char *pValue, const int value_len);
typedef int (*func_partial_set)(StoreHandle *pHandle, const char *pKey, \
		const int key_len, const char *pValue, const int offset, \
		const int value_len);
typedef int (*func_delete)(StoreHandle *pHandle, const char *pKey, \
		const int key_len);
typedef int (*func_inc)(StoreHandle *pHandle, const char *pKey, \
		const int key_len, const int inc, char *pValue, int *value_len);

typedef int (*func_inc_ex)(StoreHandle *pHandle, const char *pKey, \
		const int key_len, const int inc, char *pValue, \
		int *value_len, const int timeout);

typedef int (*func_clear_expired_keys)(void *arg);

extern func_destroy_instance g_func_destroy_instance;
extern func_destroy g_func_destroy;
extern func_memp_trickle g_func_memp_trickle;
extern func_get g_func_get;
extern func_set g_func_set;
extern func_partial_set g_func_partial_set;
extern func_delete g_func_delete;
extern func_inc g_func_inc;
extern func_inc_ex g_func_inc_ex;
extern func_clear_expired_keys g_func_clear_expired_keys;

void store_init();

#ifdef __cplusplus
}
#endif

#endif

