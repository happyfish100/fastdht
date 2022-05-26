//mpool_op.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include "logger.h"
#include "global.h"
#include "shared_func.h"
#include "sched_thread.h"
#include "mpool_op.h"

HashArray *g_hash_array = NULL;
static pthread_rwlock_t mpool_pthread_rwlock;
static bool have_htable_lock = false;

int mp_init(StoreHandle **ppHandle, const u_int64_t nCacheSize)
{
	int result;
	if (g_hash_array != NULL)
	{
		*ppHandle = g_hash_array;
		return 0;
	}

	g_hash_array = (HashArray *)malloc(sizeof(HashArray));
	if (g_hash_array == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(HashArray), \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	if ((result=fc_hash_init_ex(g_hash_array, Time33Hash, g_mpool_init_capacity,\
		g_mpool_load_factor, nCacheSize, true)) != 0)
	{
		return result;
	}
	fc_hash_set_locks(g_hash_array, g_mpool_htable_lock_count);
	have_htable_lock = g_hash_array->lock_count > 0;
	*ppHandle = g_hash_array;

	if ((result=pthread_rwlock_init(&mpool_pthread_rwlock, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_rwlock_init fail, errno: %d, error info: %s",\
			__LINE__, result, STRERROR(result));
		return result;
	}

	return 0;
}

int mp_destroy_instance(StoreHandle **ppHandle)
{
	return 0;
}

int mp_destroy()
{
	pthread_rwlock_destroy(&mpool_pthread_rwlock);

	return 0;
}

int mp_memp_trickle(int *nwrotep)
{
	*nwrotep = 0;
	return 0;
}

#define RWLOCK_READ_LOCK(result) \
	if (!have_htable_lock && (result=pthread_rwlock_rdlock( \
			&mpool_pthread_rwlock)) != 0) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"pthread_rwlock_rdlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result)); \
		return result; \
	}

#define RWLOCK_WRITE_LOCK(result) \
	if (!have_htable_lock && (result=pthread_rwlock_wrlock( \
			&mpool_pthread_rwlock)) != 0) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"pthread_rwlock_rdlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result)); \
		return result; \
	}

#define RWLOCK_UNLOCK(result) \
	if (!have_htable_lock && (result=pthread_rwlock_unlock( \
			&mpool_pthread_rwlock)) != 0) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"pthread_rwlock_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result)); \
	}

int mp_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	HashData *hash_data;
	int result;
	int lock_result;

	g_server_stat.total_get_count++;

	RWLOCK_READ_LOCK(lock_result)

	do
	{
		hash_data = fc_hash_find_ex(g_hash_array, pKey, key_len);
		if (hash_data == NULL)
		{
			result = ENOENT;
			break;
		}

		if (*ppValue != NULL)
		{
			if (*size < hash_data->value_len)
			{
				*size = hash_data->value_len;
				result = ENOSPC;
				break;
			}

			*size = hash_data->value_len;
			memcpy(*ppValue, hash_data->value, hash_data->value_len);
			g_server_stat.success_get_count++;
			result = 0;
			break;
		}

		*size = hash_data->value_len;
		*ppValue = (char *)malloc(hash_data->value_len);
		if (*ppValue == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, hash_data->value_len, \
				errno, STRERROR(errno));
			result = errno != 0 ? errno : ENOMEM;
			break;
		}

		memcpy(*ppValue, hash_data->value, hash_data->value_len);
		g_server_stat.success_get_count++;
		result = 0;
	} while (0);

	RWLOCK_UNLOCK(lock_result)

	return result;
}

static int mp_do_set(StoreHandle *pHandle, const char *pKey, const int key_len,\
	const char *pValue, const int value_len)
{
	int result;
	int i;

	for (i=0; i<2; i++)
	{
		result = fc_hash_insert_ex(pHandle, pKey, key_len, \
				(void *)pValue, value_len, true);
		if (result < 0)
		{
			result *= -1;
			if (result == ENOSPC && g_need_clear_expired_data)
			{
				int lock_result;

				RWLOCK_UNLOCK(lock_result)
				if (mp_clear_expired_keys(pHandle) > 0)
				{
					RWLOCK_WRITE_LOCK(lock_result)
					continue;
				}
				RWLOCK_WRITE_LOCK(lock_result)
			}

			break;
		}
		else if (result == 0)
		{
			break;
		}
		else
		{
			result = 0;
			break;
		}
	}

	return result;
}

int mp_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	int lock_result;

	g_server_stat.total_set_count++;

	RWLOCK_WRITE_LOCK(lock_result)

	result = mp_do_set(g_hash_array, pKey, key_len, pValue, value_len);
	if (result == 0)
	{
		g_server_stat.success_set_count++;
	}

	RWLOCK_UNLOCK(lock_result)

	return result;
}

int mp_partial_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int offset, const int value_len)
{
	int result;
	int lock_result;

	RWLOCK_WRITE_LOCK(lock_result)

	result = fc_hash_partial_set(g_hash_array, pKey, key_len,
			pValue, offset, value_len);

	RWLOCK_UNLOCK(lock_result)

	return result;
}

int mp_delete(StoreHandle *pHandle, const char *pKey, const int key_len)
{
	int result;
	int lock_result;

	g_server_stat.total_delete_count++;
	
	RWLOCK_WRITE_LOCK(lock_result)

	result = fc_hash_delete(g_hash_array, pKey, key_len);
	if (result == 0)
	{
		g_server_stat.success_delete_count++;
	}

	RWLOCK_UNLOCK(lock_result)

	return result;
}

int mp_inc(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len)
{
	int result;
	int lock_result;

	g_server_stat.total_inc_count++;

	RWLOCK_WRITE_LOCK(lock_result)

	result = fc_hash_inc(g_hash_array, pKey, key_len, inc,
			pValue, value_len);
	if (result == 0)
	{
		g_server_stat.success_inc_count++;
	}

	RWLOCK_UNLOCK(lock_result)

	return result;
}

static int64_t mp_inc_value(const HashData *old_data, const int inc,
    char *new_value, int *new_value_len, void *arg)
{
	int64_t n;
	int old_expires;
	int expires;
	if (old_data == NULL)
	{
		n = inc;
	}
	else
	{
		old_expires = buff2int(old_data->value);
		if (old_expires != FDHT_EXPIRES_NEVER && \
			old_expires < g_current_time) //expired
		{
			n = inc;
		}
		else
		{
			if (old_data->value_len >= *new_value_len)
			{
				n = inc;
			}
			else
			{
				memcpy(new_value, old_data->value, \
					old_data->value_len);
				new_value[old_data->value_len] = '\0';
				n = strtoll(new_value + 4, NULL, 10);
				n += inc;
			}
		}
	}
	expires = (int)((long)arg);
	int2buff(expires, new_value);
	*new_value_len = 4 + sprintf(new_value + 4, "%"PRId64, n);
	return n;
}

int mp_inc_ex(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int expires)
{
	int result;
	int lock_result;

	g_server_stat.total_inc_count++;

	RWLOCK_WRITE_LOCK(lock_result)

	result = fc_hash_inc_ex(g_hash_array, pKey, key_len, inc, pValue,
		value_len, mp_inc_value, (void *)((long)expires));
	if (result == 0)
	{
		g_server_stat.success_inc_count++;
	}

	RWLOCK_UNLOCK(lock_result)

	return result;
}

int mp_clear_expired_keys(void *arg)
{
	HashData **ppBucket;
	HashData **bucket_end;
	HashData *hash_data;
	HashData *pDeleted;
	HashData *previous;
	time_t current_time;
	int expires;
	int lock_result;
	struct timeval tv_start;
	struct timeval tv_end;
	unsigned int bucket_index;
	int old_item_count;
	static time_t last_clear_time = 0;
	static bool clearing = false;

	if (clearing)
	{
		logInfo("file: "__FILE__", line: %d, " \
			"clear proccess already running", __LINE__);
		return 0;
	}

	if (gettimeofday(&tv_start, NULL) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call gettimeofday fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		return -1;
	}

	current_time = tv_start.tv_sec;
	if (current_time - last_clear_time < g_mpool_clear_min_interval)
	{
		return 0;
	}

	pthread_rwlock_wrlock(&mpool_pthread_rwlock);
	if (clearing)
	{
		logInfo("file: "__FILE__", line: %d, " \
			"clear proccess already running", __LINE__);

		pthread_rwlock_unlock(&mpool_pthread_rwlock);
		return 0;
	}
	clearing = true;
	pthread_rwlock_unlock(&mpool_pthread_rwlock);

	RWLOCK_WRITE_LOCK(lock_result)

	old_item_count = g_hash_array->item_count;
	bucket_end = g_hash_array->buckets + (*g_hash_array->capacity);
	for (ppBucket=g_hash_array->buckets; ppBucket<bucket_end; ppBucket++)
	{
		bucket_index = ppBucket - g_hash_array->buckets;
		fc_hash_bucket_lock(g_hash_array, bucket_index);
		if (*ppBucket == NULL)
		{
			fc_hash_bucket_unlock(g_hash_array, bucket_index);
			continue;
		}

		hash_data = *ppBucket;
		previous = NULL;
		do
		{
			expires = buff2int(hash_data->value);
			if (expires == FDHT_EXPIRES_NEVER || \
				expires > current_time)
			{
				if (previous == NULL)
				{
					*ppBucket = hash_data;
				}
				else
				{
					previous->next = hash_data;
				}

				previous = hash_data;
				hash_data = hash_data->next;
			}
			else
			{
				pDeleted = hash_data;
				hash_data = hash_data->next;

				FREE_HASH_DATA(g_hash_array, pDeleted)
			}

		} while (hash_data != NULL);

		if (previous == NULL)
		{
			*ppBucket = NULL;
		}
		else
		{
			previous->next = NULL;
		}
		fc_hash_bucket_unlock(g_hash_array, bucket_index);
	}

	RWLOCK_UNLOCK(lock_result)

	gettimeofday(&tv_end, NULL);

	logInfo("clear expired keys, total key count: %d, " \
		"expired key count: %d, time used: %dms, mpool free bytes: " \
		INT64_PRINTF_FORMAT" bytes", \
		old_item_count, old_item_count - g_hash_array->item_count, \
		(int)((tv_end.tv_sec - tv_start.tv_sec) * 1000 + \
		(tv_end.tv_usec - tv_start.tv_usec) / 1000), \
		g_hash_array->max_bytes - g_hash_array->bytes_used);

	clearing = false;
	last_clear_time = tv_end.tv_sec;

	return old_item_count - g_hash_array->item_count;
}

