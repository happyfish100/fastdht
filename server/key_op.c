#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "key_op.h"
#include "global.h"
#include "func.h"
#include "sync.h"

bool g_store_sub_keys = false;
static pthread_mutex_t *locks = NULL;
static int lock_count = 0;

int key_init()
{
	pthread_mutex_t *pLock;
	pthread_mutex_t *lock_end;
	int result;

	if (!g_store_sub_keys)
	{
		return 0;
	}

	lock_count = SF_G_WORK_THREADS;
	if (lock_count % 2 == 0)
	{
		lock_count += 1;
	}

	locks = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t) * lock_count);
	if (locks == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(pthread_mutex_t) * lock_count, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	lock_end = locks + lock_count;
	for (pLock=locks; pLock<lock_end; pLock++)
	{
		if ((result=pthread_mutex_init(pLock, NULL)) != 0)
		{
			return result;
		}
	}

	return 0;
}

void key_destroy()
{
	pthread_mutex_t *pLock;
	pthread_mutex_t *lock_end;
	if (!g_store_sub_keys)
	{
		return;
	}

	if (locks == NULL)
	{
		return;
	}

	lock_end = locks + lock_count;
	for (pLock=locks; pLock<lock_end; pLock++)
	{
		pthread_mutex_destroy(pLock);
	}

	free(locks);
	locks = NULL;
	return;
}

static int key_do_get(StoreHandle *pHandle, const char *full_key, \
		const int full_key_len, char *key_list, int *value_len, \
		char **key_array, int *key_count)
{
	int result;

	*value_len = 4 + FDHT_KEY_LIST_MAX_SIZE - 1;
	result = g_func_get(pHandle, full_key, full_key_len, \
				&key_list, value_len);
	if (result == ENOENT)
	{
		*value_len = 0;
	}
	else if (result != 0)
	{
		return result;
	}

	if (*value_len <= 4)
	{
		*value_len = 0;
		*(key_list + *value_len) = '\0';
		*key_count = 0;
		return 0;
	}

	*(key_list + *value_len) = '\0';
	*key_count = splitEx(key_list + 4, FDHT_FULL_KEY_SEPERATOR, \
				key_array, *key_count);
	return 0;
}

static int key_compare(const void *p1, const void *p2)
{
	return strcmp(*((const char **)p1), *((const char **)p2));
}

static int key_do_set(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		const int key_hash_code, const char *full_key, \
		const int full_key_len, char *value, int value_len)
{
	int result;
	int op_type;
	int expire;

	expire = FDHT_EXPIRES_NEVER;
	if (value_len > 4)
	{
		op_type = FDHT_OP_TYPE_SOURCE_SET;
		int2buff(expire, value);
		result = g_func_set(pHandle, full_key, full_key_len, \
					value, value_len);
	}
	else
	{
		value_len = 4;
		op_type = FDHT_OP_TYPE_SOURCE_DEL;
		result = g_func_delete(pHandle, full_key, full_key_len);
	}

	if (result != 0)
	{
		return result;
	}

	if (g_write_to_binlog_flag)
	{
		return fdht_binlog_write(g_current_time, \
			op_type, key_hash_code, expire, \
			pKeyInfo, value + 4, value_len - 4);
	}
	else
	{
		return result;
	}
}

static int key_do_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
			const int key_hash_code)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppTargetKey;
	char **ppFound;
	FDHTKeyInfo keyInfo2log;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int result;
	int expire;
	int i, k;

	memcpy(&keyInfo2log, pKeyInfo, sizeof(FDHTKeyInfo));
	keyInfo2log.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(keyInfo2log.szKey, FDHT_LIST_KEY_NAME_STR, \
			FDHT_LIST_KEY_NAME_LEN + 1);
	FDHT_PACK_FULL_KEY(keyInfo2log, full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_do_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	/*
	logInfo("file: "__FILE__", line: %d: value len=%d, key_count=%d", \
		__LINE__, value_len, key_count);
	for (i=0; i<key_count; i++)
	{
		logInfo("file: "__FILE__", line: %d: %d. key=%s", __LINE__, \
			i+1, key_array[i]);
	}
	*/

	p = pKeyInfo->szKey;
	ppTargetKey = &p;
	ppFound = (char **)bsearch(ppTargetKey, key_array, key_count, \
			sizeof(char *), key_compare);
	if (ppFound != NULL)
	{
		return 0;
	}

	if (value_len + 1 + pKeyInfo->key_len >= FDHT_KEY_LIST_MAX_SIZE)
	{
		return ENOSPC;
	}

	p = new_key_list + 3;
	for (i=0; i<key_count; i++)
	{
		if (strcmp(pKeyInfo->szKey, key_array[i]) < 0)
		{
			break;
		}

		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	*p++ = FDHT_FULL_KEY_SEPERATOR;
	memcpy(p, pKeyInfo->szKey, pKeyInfo->key_len);
	p += pKeyInfo->key_len;

	for (k=i; k<key_count; k++)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(key_array[k]);
		memcpy(p, key_array[k], key_len);
		p += key_len;
	}

	expire = FDHT_EXPIRES_NEVER;
	int2buff(expire, new_key_list);
	value_len = p - new_key_list;
	result = g_func_set(pHandle, full_key, full_key_len, \
			new_key_list, value_len);
	if (result != 0)
	{
		return result;
	}

	if (g_write_to_binlog_flag)
	{
		return fdht_binlog_write(g_current_time, \
			FDHT_OP_TYPE_SOURCE_SET, \
			key_hash_code, expire, &keyInfo2log, \
			new_key_list + 4, value_len - 4);
	}
	else
	{
		return result;
	}
}

static int key_do_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
			const int key_hash_code)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppTargetKey;
	char **ppFound;
	FDHTKeyInfo keyInfo2log;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int index;
	int result;
	int i;

	memcpy(&keyInfo2log, pKeyInfo, sizeof(FDHTKeyInfo));
	keyInfo2log.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(keyInfo2log.szKey, FDHT_LIST_KEY_NAME_STR, \
		FDHT_LIST_KEY_NAME_LEN + 1);
	FDHT_PACK_FULL_KEY(keyInfo2log, full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_do_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	p = pKeyInfo->szKey;
	ppTargetKey = &p;
	ppFound = (char **)bsearch(ppTargetKey, key_array, key_count, \
			sizeof(char *), key_compare);
	if (ppFound == NULL)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"namespace: %s, object id: %s, key: %s not exist!", \
			__LINE__, pKeyInfo->szNameSpace, \
			pKeyInfo->szObjectId, pKeyInfo->szKey);
		return 0;
	}

	index = ppFound - key_array;
	p = new_key_list + 3;
	for (i=0; i<index; i++)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	for (i=index+1; i<key_count; i++)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	value_len = p - new_key_list;
	return key_do_set(pHandle, &keyInfo2log, \
		key_hash_code, full_key, \
		full_key_len, new_key_list, value_len);
}

static int key_batch_do_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	int add_key_count;
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppKey;
	char **ppKeyEnd;
	FDHTSubKey *pSubKey;
	FDHTSubKey *pSubEnd;
	char *pPreviousKey;
	FDHTKeyInfo keyInfo2log;
	int full_key_len;
	int key_len;
	int value_len;
	int total_len;
	int key_count;
	int result;
	int compare;
	int expire;

	if (sub_key_count == 0)
	{
		return 0;
	}
	if (sub_key_count == 1)
	{
		pKeyInfo->key_len = subKeys->key_len;
		memcpy(pKeyInfo->szKey, subKeys->szKey, subKeys->key_len);
		*(pKeyInfo->szKey + pKeyInfo->key_len) = '\0';
		return key_do_add(pHandle, pKeyInfo, key_hash_code);
	}

	memcpy(&keyInfo2log, pKeyInfo, sizeof(FDHTKeyInfo));
	keyInfo2log.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(keyInfo2log.szKey, FDHT_LIST_KEY_NAME_STR, \
		FDHT_LIST_KEY_NAME_LEN + 1);
	FDHT_PACK_FULL_KEY(keyInfo2log, full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_do_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	/*
	{
	int i;
	logInfo("file: "__FILE__", line: %d: value len=%d, key_count=%d", \
		__LINE__, value_len, key_count);
	for (i=0; i<key_count; i++)
	{
		logInfo("file: "__FILE__", line: %d: %d. key=%s", __LINE__, \
			i+1, key_array[i]);
	}
	}
	*/

	total_len = 0;
	pSubEnd = subKeys + sub_key_count;
	for (pSubKey=subKeys; pSubKey<pSubEnd; pSubKey++)
	{
		total_len += 1 + pSubKey->key_len;
	}

	if (value_len + total_len >= FDHT_KEY_LIST_MAX_SIZE)
	{
		return ENOSPC;
	}

	add_key_count = 0;
	p = new_key_list + 3;
	pSubKey = subKeys;
	ppKey = key_array;
	ppKeyEnd = key_array + key_count;
	while (pSubKey < pSubEnd && ppKey < ppKeyEnd)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		compare = strcmp(pSubKey->szKey, *ppKey);
		if (compare < 0)
		{
			memcpy(p, pSubKey->szKey, pSubKey->key_len);
			p += pSubKey->key_len;
			add_key_count++;
		}
		else if (compare == 0)
		{
			memcpy(p, pSubKey->szKey, pSubKey->key_len);
			p += pSubKey->key_len;

			ppKey++;
		}
		else
		{
			key_len = strlen(*ppKey);
			memcpy(p, *ppKey, key_len);
			p += key_len;

			ppKey++;
			continue;
		}

		pPreviousKey = pSubKey->szKey;
		pSubKey++;
		while (pSubKey < pSubEnd)
		{
			if (strcmp(pSubKey->szKey, pPreviousKey) != 0)
			{
				break;
			}

			pSubKey++;
		}
	}

	while (pSubKey < pSubEnd)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		memcpy(p, pSubKey->szKey, pSubKey->key_len);
		p += pSubKey->key_len;
		add_key_count++;

		pPreviousKey = pSubKey->szKey;
		pSubKey++;
		while (pSubKey < pSubEnd)
		{
			if (strcmp(pSubKey->szKey, pPreviousKey) != 0)
			{
				break;
			}

			pSubKey++;
		}
	}

	if (add_key_count == 0)  //no new key added
	{
		return 0;
	}

	while (ppKey < ppKeyEnd)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(*ppKey);
		memcpy(p, *ppKey, key_len);
		p += key_len;

		ppKey++;
	}

	expire = FDHT_EXPIRES_NEVER;
	int2buff(expire, new_key_list);
	value_len = p - new_key_list;
	result = g_func_set(pHandle, full_key, full_key_len, \
			new_key_list, value_len);
	if (result != 0)
	{
		return result;
	}

	if (g_write_to_binlog_flag)
	{
		return fdht_binlog_write(g_current_time, \
			FDHT_OP_TYPE_SOURCE_SET, \
			key_hash_code, expire, &keyInfo2log, \
			new_key_list + 4, value_len - 4);
	}
	else
	{
		return result;
	}
}

static int key_batch_do_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[4 + FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppKey;
	char **ppKeyEnd;
	FDHTSubKey *pSubKey;
	FDHTSubKey *pSubEnd;
	FDHTKeyInfo keyInfo2log;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int result;
	int compare;

	if (sub_key_count == 0)
	{
		return 0;
	}
	if (sub_key_count == 1)
	{
		pKeyInfo->key_len = subKeys->key_len;
		memcpy(pKeyInfo->szKey, subKeys->szKey, subKeys->key_len);
		*(pKeyInfo->szKey + pKeyInfo->key_len) = '\0';
		return key_do_del(pHandle, pKeyInfo, key_hash_code);
	}

	memcpy(&keyInfo2log, pKeyInfo, sizeof(FDHTKeyInfo));
	keyInfo2log.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(keyInfo2log.szKey, FDHT_LIST_KEY_NAME_STR, \
		FDHT_LIST_KEY_NAME_LEN + 1);
	FDHT_PACK_FULL_KEY(keyInfo2log, full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_do_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	p = new_key_list + 3;
	pSubKey = subKeys;
	ppKey = key_array;
	pSubEnd = subKeys + sub_key_count;
	ppKeyEnd = key_array + key_count;
	while (pSubKey < pSubEnd && ppKey < ppKeyEnd)
	{
		compare = strcmp(pSubKey->szKey, *ppKey);
		if (compare < 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"namespace: %s, object id: %s, " \
				"key: %s not exist!", \
				__LINE__, pKeyInfo->szNameSpace, \
				pKeyInfo->szObjectId, pSubKey->szKey);
			pSubKey++;
		}
		else if (compare == 0)
		{
			pSubKey++;
			ppKey++;
		}
		else
		{
			*p++ = FDHT_FULL_KEY_SEPERATOR;
			key_len = strlen(*ppKey);
			memcpy(p, *ppKey, key_len);
			p += key_len;

			ppKey++;
		}
	}

	while (pSubKey < pSubEnd)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"namespace: %s, object id: %s, " \
			"key: %s not exist!", \
			__LINE__, pKeyInfo->szNameSpace, \
			pKeyInfo->szObjectId, pSubKey->szKey);

		pSubKey++;
	}

	while (ppKey < ppKeyEnd)
	{
		*p++ = FDHT_FULL_KEY_SEPERATOR;
		key_len = strlen(*ppKey);
		memcpy(p, *ppKey, key_len);
		p += key_len;

		ppKey++;
	}

	value_len = p - new_key_list;
	return key_do_set(pHandle, &keyInfo2log, \
		key_hash_code, full_key, \
		full_key_len, new_key_list, value_len);
}

int key_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		const int key_hash_code)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_do_add(pHandle, pKeyInfo, key_hash_code);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		const int key_hash_code)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_do_del(pHandle, pKeyInfo, key_hash_code);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_batch_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_batch_do_add(pHandle, pKeyInfo, key_hash_code, \
				subKeys, sub_key_count);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_batch_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_batch_do_del(pHandle, pKeyInfo, key_hash_code, \
				subKeys, sub_key_count);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_get(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		char *key_list, int *keys_len)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	int result;
	FDHTKeyInfo keyInfo2log;

	if (!g_store_sub_keys)
	{
		return EOPNOTSUPP;
	}

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return EINVAL;
	}

	memcpy(&keyInfo2log, pKeyInfo, sizeof(FDHTKeyInfo));
	keyInfo2log.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(keyInfo2log.szKey, FDHT_LIST_KEY_NAME_STR, \
		FDHT_LIST_KEY_NAME_LEN + 1);
	FDHT_PACK_FULL_KEY(keyInfo2log, full_key, full_key_len, p)
	result = g_func_get(pHandle, full_key, full_key_len, \
				&key_list, keys_len);
	if (result != 0)
	{
		return result;
	}

	return *keys_len > 4 ? 0 : ENOENT;
}

