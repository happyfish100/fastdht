//recovery.c

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "fdht_global.h"
#include "global.h"
#include "fdht_func.h"
#include "fast_task_queue.h"
#include "sync.h"
#include "func.h"
#include "db_recovery.h"

#define LOCAL_DB_SYNC_MARK_FILENAME	"db_recovery_mark.dat"
#define MARK_ITEM_BINLOG_FILE_INDEX	"binlog_index"
#define MARK_ITEM_BINLOG_FILE_OFFSET	"binlog_offset"
#define MARK_ITEM_START_TIME		"start_time"
#define MARK_ITEM_SYNC_TIME_USED	"time_used"
#define MARK_ITEM_WRITTEN_PAGES		"written_pages"

static int fdht_db_recovery_mark_fd = -1;

static char *fdht_get_db_recovery_mark_filename(const void *pArg, \
			char *full_filename);
static int fdht_write_to_db_recovery_mark_file(const time_t timestamp, \
		const int binlog_index, const int64_t binlog_offset, \
		const int written_pages, const int time_used_ms);
static int fdht_recover_data(const int start_binlog_index, \
		const int64_t start_binlog_offset);

int fdht_db_recovery_init()
{
	int result;
	char full_filename[MAX_PATH_SIZE];
	IniContext iniContext;
	char *pValue;
	int synced_binlog_index;
	int64_t synced_binlog_offset;
	bool bMarkFileExists;
	bool bRecovery;
	time_t start_time;
	struct timeval tvStart;
	struct timeval tvEnd;

	fdht_get_db_recovery_mark_filename(NULL, full_filename);
	if ((bMarkFileExists=fileExists(full_filename)))
	{
		if ((result=iniLoadFromFile(full_filename, \
				&iniContext)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from file \"%s\" fail, " \
				"error code: %d, error info: %s", \
				__LINE__, full_filename, \
				result, STRERROR(result));
			return result;
		}

		pValue = iniGetStrValue(NULL, MARK_ITEM_BINLOG_FILE_INDEX, \
				&iniContext);
		if (pValue == NULL)
		{
			iniFreeContext(&iniContext);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s\", item \"%s\" not exists", \
				__LINE__, full_filename, \
				MARK_ITEM_BINLOG_FILE_INDEX);
			return ENOENT;
		}
		synced_binlog_index = atoi(pValue);

		pValue = iniGetStrValue(NULL, MARK_ITEM_BINLOG_FILE_OFFSET, \
				&iniContext);
		if (pValue == NULL)
		{
			iniFreeContext(&iniContext);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s\", item \"%s\" not exists", \
				__LINE__, full_filename, \
				MARK_ITEM_BINLOG_FILE_OFFSET);
			return ENOENT;
		}
		synced_binlog_offset = strtoll(pValue, NULL, 10);

		iniFreeContext(&iniContext);
	}
	else
	{
		synced_binlog_index = 0;
		synced_binlog_offset = 0;
	}

	fdht_db_recovery_mark_fd = open(full_filename, O_WRONLY | O_CREAT, 0644);
	if (fdht_db_recovery_mark_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open local db sync mark file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	gettimeofday(&tvStart, NULL);
	start_time = tvStart.tv_sec;

	if ((synced_binlog_index < g_binlog_index) || \
		(synced_binlog_offset < g_binlog_file_size))
	{
		bRecovery = true;
		result = fdht_recover_data(synced_binlog_index, \
					synced_binlog_offset);
	}
	else
	{
		bRecovery = false;
		result = 0;
	}

	if (result == 0 && (!bMarkFileExists || bRecovery))
	{
		gettimeofday(&tvEnd, NULL);
		result = fdht_write_to_db_recovery_mark_file(start_time, \
			g_binlog_index, g_binlog_file_size, 0, \
			1000 * (tvEnd.tv_sec - tvStart.tv_sec) + \
			(tvEnd.tv_usec - tvStart.tv_usec) / 1000);
	}

	return result;
}

int fdht_memp_trickle_dbs(void *args)
{
	int written_pages;
	int total_written_pages;
	int fail_count;
	time_t start_time;
	struct timeval tvStart;
	struct timeval tvEnd;
	int current_binlog_index;
	int64_t current_binlog_offset;

	gettimeofday(&tvStart, NULL);
	start_time = tvStart.tv_sec;
	current_binlog_index = g_binlog_index;
	current_binlog_offset = g_binlog_file_size;

	fail_count = 0;
	total_written_pages = 0;
	if (db_memp_trickle(&written_pages) == 0)
	{
		total_written_pages += written_pages;
	}
	else
	{
		fail_count++;
	}

	if (((fail_count == 0) && (total_written_pages > 0)) || \
		(args != NULL && (long)args == 1))
	{
		gettimeofday(&tvEnd, NULL);
		fdht_write_to_db_recovery_mark_file(start_time, \
			current_binlog_index, current_binlog_offset, \
			total_written_pages, \
			1000 * (tvEnd.tv_sec - tvStart.tv_sec) + \
			(tvEnd.tv_usec - tvStart.tv_usec) / 1000);
	}

	logInfo("db_sync total_written_pages=%d", total_written_pages);
	return total_written_pages;
}

static char *fdht_get_db_recovery_mark_filename(const void *pArg, \
			char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/%s", g_fdht_base_path, \
			LOCAL_DB_SYNC_MARK_FILENAME);
	return full_filename;
}

static int fdht_write_to_db_recovery_mark_file(const time_t timestamp, \
		const int binlog_index, const int64_t binlog_offset, \
		const int written_pages, const int time_used_ms)
{
	char buff[256];
	char date_buff[32];
	int len;

	len = sprintf(buff, \
		"%s=%d\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s=%d\n"  \
		"%s=%s\n"  \
		"%s=%d ms\n",  \
		MARK_ITEM_BINLOG_FILE_INDEX, binlog_index, \
		MARK_ITEM_BINLOG_FILE_OFFSET, binlog_offset, \
		MARK_ITEM_WRITTEN_PAGES, written_pages, \
		MARK_ITEM_START_TIME, formatDatetime(timestamp, \
			"%Y-%m-%d %H:%M:%S", date_buff, sizeof(date_buff)), \
		MARK_ITEM_SYNC_TIME_USED, time_used_ms);

	return fdht_write_to_fd(fdht_db_recovery_mark_fd, \
			fdht_get_db_recovery_mark_filename, NULL, buff, len);
}

#define CHECK_GROUP_ID(pRecord, group_id) \
	group_id = ((unsigned int)pRecord->key_hash_code) % g_group_count; \
	if (group_id >= g_db_count) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"invalid group_id: %d, " \
			"which < 0 or >= %d", \
			__LINE__, group_id, g_db_count); \
		return  EINVAL; \
	} \
	if (g_db_list[group_id] == NULL) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"invalid group_id: %d, " \
			"which does not belong to this server", \
			__LINE__, group_id); \
		return  EINVAL; \
	} \

static int recover_cmd_set(BinLogRecord *pRecord, BinField *pFullValue)
{
	int group_id;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	char *p;  //tmp var

	CHECK_GROUP_ID(pRecord, group_id)

	if (pRecord->value.length + 5 > pFullValue->size)
	{
		p = pFullValue->data;
		pFullValue->size = pRecord->value.length + 1024;
		pFullValue->data = (char *)malloc(pFullValue->size);
		if (pFullValue->data == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", __LINE__, \
				pFullValue->size, errno, STRERROR(errno));

			
			pFullValue->data = p;
			return errno != 0 ? errno : ENOMEM;
		}

		free(p);
	}

	FDHT_PACK_FULL_KEY(pRecord->key_info, full_key, full_key_len, p)

	int2buff(pRecord->expires, pFullValue->data);
	memcpy(pFullValue->data+4, pRecord->value.data, pRecord->value.length);
	pFullValue->length = 4 + pRecord->value.length;
	return db_set(g_db_list[group_id], full_key, full_key_len, \
			pFullValue->data, pFullValue->length);
}

static int recover_cmd_del(BinLogRecord *pRecord)
{
	int group_id;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	char *p;  //tmp var

	CHECK_GROUP_ID(pRecord, group_id)
	FDHT_PACK_FULL_KEY(pRecord->key_info, full_key, full_key_len, p)

	return db_delete(g_db_list[group_id], full_key, full_key_len);
}

static int fdht_recover_data(const int start_binlog_index, \
		const int64_t start_binlog_offset)
{
	int result;
	BinLogReader reader;
	BinLogRecord record;
	BinField full_value;
	int record_len;
	char *p;
	char *pEnd;

	memset(&reader, 0, sizeof(BinLogReader));
	memset(&record, 0, sizeof(BinLogRecord));

	pEnd = g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count;
        for (p=g_local_host_ip_addrs; p<pEnd; p+=IP_ADDRESS_SIZE)
	{
		if (strcmp(p, "127.0.0.1") != 0)
		{
			strcpy(reader.ip_addr, p);
			break;
		}
	}

	reader.port = g_server_port;
	reader.binlog_index = start_binlog_index;
	reader.binlog_offset = start_binlog_offset;
	reader.binlog_fd = -1;
	reader.mark_fd = fdht_db_recovery_mark_fd;
	if ((result=fdht_open_readable_binlog(&reader)) != 0)
	{
		return result;
	}

	full_value.size = 64 * 1024;
	full_value.length = 0;
	full_value.data = (char *)malloc(full_value.size);
	if (full_value.data == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", __LINE__, \
			full_value.size, errno, STRERROR(errno));

		return errno != 0 ? errno : ENOMEM;
	}

	while (g_continue_flag)
	{
		result = fdht_binlog_read(&reader, \
				&record, &record_len);
		if (result != 0)
		{
			break;
		}

		switch(record.op_type)
		{
			case FDHT_OP_TYPE_SOURCE_SET:
			case FDHT_OP_TYPE_REPLICA_SET:
				result = recover_cmd_set(&record, &full_value);
				break;
			case FDHT_OP_TYPE_SOURCE_DEL:
			case FDHT_OP_TYPE_REPLICA_DEL:
				result = recover_cmd_del(&record);
				break;
			default:
				return EINVAL;
		}
		
		reader.scan_row_count++;
		if (result == 0)
		{
			reader.sync_row_count++;
		}
		else if (result != ENOENT)
		{
			break;
		}
	}

	if (record.value.data != NULL)
	{
		free(record.value.data);
	}

	if (full_value.data != NULL)
	{
		free(full_value.data);
	}

	logInfo("file: "__FILE__", line: %d, " \
		"recover data, scan row count: "INT64_PRINTF_FORMAT", " \
		"success recover count: "INT64_PRINTF_FORMAT, \
		__LINE__, reader.scan_row_count, reader.sync_row_count);

	return result != ENOENT ? result : 0;
}

