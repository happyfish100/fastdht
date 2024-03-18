#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <db.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "db_op.h"
#include "global.h"
#include "func.h"

static DB_ENV *g_db_env = NULL;

static void db_errcall(const DB_ENV *dbenv, const char *errpfx, const char *msg)
{
	logError("file: "__FILE__", line: %d, " \
		"db error, error info: %s", \
		__LINE__, msg);
}

static int db_env_init(DB_ENV **ppDBEnv, const u_int64_t nCacheSize, \
		const u_int32_t page_size, const char *base_path)
{
#define _DB_BLOCK_BYTES   (256 * 1024 * 1024)
	int result;
	u_int32_t gb;
	u_int32_t bytes;
	int blocks;
	char *sub_dirs[] = {"tmp", "logs", "data"};
	char full_path[256];
	int i;

	for (i=0; i<sizeof(sub_dirs)/sizeof(char *); i++)
	{
		snprintf(full_path, sizeof(full_path), "%s/%s", \
			base_path, sub_dirs[i]);
		if (!fileExists(full_path))
		{
			if (mkdir(full_path, 0755) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"mkdir %s fail, " \
					"errno: %d, error info: %s", \
					__LINE__, full_path, \
					errno, STRERROR(errno));
				return errno != 0 ? errno : EPERM;
			}
		}
	}

	if ((result=db_env_create(ppDBEnv, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_env_create fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=(*ppDBEnv)->set_alloc((*ppDBEnv), malloc, realloc, free)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_alloc fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	(*ppDBEnv)->set_tmp_dir((*ppDBEnv), "tmp");
	(*ppDBEnv)->set_lg_dir((*ppDBEnv), "logs");
	(*ppDBEnv)->set_data_dir((*ppDBEnv), "data");
	(*ppDBEnv)->set_errcall((*ppDBEnv), db_errcall);

	gb = (u_int32_t)(nCacheSize / (1024 * 1024 * 1024));
	bytes = (u_int32_t)(nCacheSize - (u_int64_t)gb  * (1024 * 1024 * 1024));
	blocks = (int)(nCacheSize / _DB_BLOCK_BYTES);
	if (nCacheSize % _DB_BLOCK_BYTES != 0)
	{
		blocks++;
	}

	//printf("gb=%d, bytes=%d, blocks=%d\n", gb, bytes, blocks);
	if ((result=(*ppDBEnv)->set_cachesize((*ppDBEnv), \
			gb, bytes, blocks)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_cachesize fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=(*ppDBEnv)->open((*ppDBEnv), base_path, \
		DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_THREAD, 0644))!=0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->open fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=(*ppDBEnv)->set_flags((*ppDBEnv), DB_TXN_NOSYNC, 1))!=0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_flags fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	return 0;
}

int db_init(StoreHandle **ppHandle, const DBType type, \
	const u_int64_t nCacheSize, const u_int32_t page_size, \
	const char *base_path, const char *filename)
{
	int result;
	DB *db;

	db = NULL;
	*ppHandle = NULL;
	if (g_db_env == NULL)
	{
		if ((result=db_env_init(&g_db_env, nCacheSize, page_size, \
					base_path)) != 0)
		{
			return result;
		}
	}

	if ((result=db_create(&db, g_db_env, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_create fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=db->set_pagesize(db, page_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db->set_pagesize, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=db->open(db, NULL, filename, NULL, \
		type, DB_CREATE | DB_THREAD, 0644)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db->open fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	*ppHandle = db;
	return 0;
}

int db_destroy_instance(StoreHandle **ppHandle)
{
	int result;

	if (*ppHandle != NULL)
	{
		if ((result=((DB *)*ppHandle)->close((DB *)*ppHandle, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_close fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}

		*ppHandle = NULL;
		return result;
	}

	return 0;
}

int db_destroy()
{
	int result;
	if (g_db_env != NULL)
	{
		if ((result=g_db_env->close(g_db_env, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_env_close fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}

		g_db_env = NULL;
		return result;
	}

	return 0;
}

int db_sync(StoreHandle *pHandle)
{
	int result;
	if (pHandle != NULL)
	{
		if ((result=((DB *)pHandle)->sync((DB *)pHandle, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_sync fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
	}
	else
	{
		result = 0;
	}

	return result;
}

int db_memp_sync()
{
	int result;

	if ((result=g_db_env->memp_sync(g_db_env, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_memp_sync fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
	}

	return result;
}

int db_memp_trickle(int *nwrotep)
{
	int result;
	if ((result=g_db_env->memp_trickle(g_db_env, 100, nwrotep)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"memp_trickle fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
	}

	//logInfo("memp_trickle %d%%, real write %d pages", 100, *nwrotep);
	return result;
}

int db_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_set_count++;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = value_len;

	if ((result=((DB *)pHandle)->put((DB *)pHandle, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_set_count++;
	return result;
}

int db_partial_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int offset, const int value_len)
{
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_set_count++;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.flags = DB_DBT_PARTIAL;
	value.data = (char *)pValue;
	value.doff = offset;
	value.dlen = value_len;
	value.size = value_len;

	if ((result=((DB *)pHandle)->put((DB *)pHandle, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_set_count++;
	return result;
}

static int _db_do_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	int result;
	DBT key;
	DBT value;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	if (*ppValue != NULL)
	{
		value.flags = DB_DBT_USERMEM;
		value.data = *ppValue;
		value.ulen = *size;
	}
	else
	{
		value.flags = DB_DBT_MALLOC;
	}

	if ((result=((DB *)pHandle)->get((DB *)pHandle, NULL, &key,  &value, 0)) != 0)
	{
		if (result == DB_NOTFOUND)
		{
			return ENOENT;
		}
		else if (result == DB_BUFFER_SMALL)
		{
			*size = value.size;
			return ENOSPC;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"db_get fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
			return EFAULT;
		}
	}

	*ppValue = value.data;
	*size = value.size;

	return result;
}

int db_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	int result;

	g_server_stat.total_get_count++;
	if ((result=_db_do_get(pHandle, pKey, key_len, \
		ppValue, size)) == 0)
	{
		g_server_stat.success_get_count++;
	}

	return result;
}

int db_delete(StoreHandle *pHandle, const char *pKey, const int key_len)
{
	int result;
	DBT key;

	g_server_stat.total_delete_count++;

	memset(&key, 0, sizeof(key));
	key.data = (char *)pKey;
	key.size = key_len;

	if ((result=((DB *)pHandle)->del((DB *)pHandle, NULL, &key, 0)) != 0)
	{
		if (result == DB_NOTFOUND)
		{
			return ENOENT;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"db_del fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
	
			return EFAULT;
		}
	}

	g_server_stat.success_delete_count++;
	return result;
}

int db_inc(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len)
{
	int64_t n;
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_inc_count++;

	if ((result=_db_do_get(pHandle, pKey, key_len, \
               	&pValue, value_len)) != 0)
	{
		if (result != ENOENT)
		{
			return result;
		}

		n = inc;
	}
	else
	{
		pValue[*value_len] = '\0';
		n = strtoll(pValue, NULL, 10);
		n += inc;
	}

	*value_len = sprintf(pValue, "%"PRId64, n);

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = *value_len;

	if ((result=((DB *)pHandle)->put((DB *)pHandle, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_inc_count++;
	return result;
}

int db_inc_ex(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int expires)
{
	int64_t n;
	int result;
	int old_expires;
	DBT key;
	DBT value;

	g_server_stat.total_inc_count++;

	if ((result=_db_do_get(pHandle, pKey, key_len, \
               	&pValue, value_len)) != 0)
	{
		if (result != ENOENT && result != ENOSPC)
		{
			return result;
		}

		n = inc;
	}
	else
	{
		old_expires = buff2int(pValue);
		if (old_expires != FDHT_EXPIRES_NEVER && \
			old_expires < g_current_time) //expired
		{
			n = inc;
		}
		else
		{
			pValue[*value_len] = '\0';
			n = strtoll(pValue+4, NULL, 10);
			n += inc;
		}
	}

	int2buff(expires, pValue);
	*value_len = 4 + sprintf(pValue+4, "%"PRId64, n);

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = *value_len;

	if ((result=((DB *)pHandle)->put((DB *)pHandle, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_inc_count++;
	return result;
}

void *bdb_dl_detect_entrance(void *arg)
{
	struct timeval t;
	int nSec;
	int nUsec;

	nSec = g_db_dead_lock_detect_interval / 1000;
	nUsec = (g_db_dead_lock_detect_interval % 1000) * 1000;

	while (g_continue_flag)
	{
		g_db_env->lock_detect(g_db_env, 0, DB_LOCK_YOUNGEST, NULL);

		t.tv_sec = nSec;
		t.tv_usec = nUsec;
		select(0, NULL, NULL, NULL, &t);
	}
	
	return NULL;
}

int db_clear_expired_keys(void *arg)
{
	int db_index;
	DB *db;
	DBC *cursor;
	int result;
	DBT key;
	DBT value;
	char szKey[FDHT_MAX_NAMESPACE_LEN + FDHT_MAX_OBJECT_ID_LEN + \
		   FDHT_MAX_SUB_KEY_LEN + 2];
	char szValue[4];
	time_t current_time;
	struct timeval tv_start;
	struct timeval tv_end;
	int64_t total_count;
	int64_t expired_count;
	int64_t success_count;
	int expires;

	if (gettimeofday(&tv_start, NULL) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call gettimeofday fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		return -1;
	}

	db_index = (long)arg;
	db = (DB *)(g_db_list[db_index]);
	if ((result=db->cursor(db, NULL, &cursor, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db->cursor fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return -1;
	}
	
	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.flags = DB_DBT_USERMEM;
	key.data = szKey;
	key.ulen = sizeof(szKey);

	value.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
	value.data = szValue;
	value.ulen = sizeof(szValue);
	value.dlen = sizeof(szValue);

	total_count = 0;
	expired_count = 0;
	success_count = 0;
	current_time = tv_start.tv_sec;

	while (g_continue_flag && (result=cursor->get(cursor, &key, &value, \
		DB_NEXT)) == 0)
	{
		/*
		((char *)key.data)[key.size] = '\0';
		logInfo("key=%s(%d), value=%d(%d)", (char *)key.data, key.size, \
			buff2int((char *)value.data), value.size);
		*/

		total_count++;

		expires = buff2int((char *)value.data);
		if (expires == FDHT_EXPIRES_NEVER || expires > current_time)
		{
			continue;
		}

		expired_count++;
		if ((result=cursor->del(cursor, 0)) == 0)
		{
			success_count++;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"cursor->del fail, errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
	}

	cursor->close(cursor);

	gettimeofday(&tv_end, NULL);

	logInfo("clear expired keys, db %d, total count: %"PRId64 \
		", expired key count: %"PRId64 \
		", success count: %"PRId64 \
		", time used: %dms", db_index + 1, \
		total_count, expired_count, success_count, \
		(int)((tv_end.tv_sec - tv_start.tv_sec) * 1000 + \
		(tv_end.tv_usec - tv_start.tv_usec) / 1000));

	return success_count;
}

