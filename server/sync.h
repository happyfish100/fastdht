/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.fastken.com/ for more detail.
**/

//sync.h

#ifndef _SYNC_H_
#define _SYNC_H_

#include "fdht_types.h"
#include "fdht_proto.h"

#define FDHT_OP_TYPE_SOURCE_SET		'S'
#define FDHT_OP_TYPE_SOURCE_DEL		'D'
#define FDHT_OP_TYPE_REPLICA_SET	's'
#define FDHT_OP_TYPE_REPLICA_DEL	'd'

#define SYNC_BINLOG_FILE_PREFIX		"binlog"
#define SYNC_BINLOG_INDEX_FILENAME	SYNC_BINLOG_FILE_PREFIX".index"
#define SYNC_MARK_FILE_EXT		".mark"
#define SYNC_BINLOG_FILE_EXT_FMT	".%05d"
#define SYNC_DIR_NAME			"sync"

#define BINLOG_FIX_FIELDS_LENGTH  4 * 10 + 3 * 4 + 1 + 8 * 1

#define CALC_RECORD_LENGTH(pKeyInfo, value_len)  BINLOG_FIX_FIELDS_LENGTH + \
			pKeyInfo->namespace_len + 1 + pKeyInfo->obj_id_len + \
			1 + pKeyInfo->key_len + 1 + value_len + 1

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char *data;
	int size;
	int length;
} BinField;

typedef struct
{
	int port;
	char ip_addr[IP_ADDRESS_SIZE];
	bool need_sync_old;
	bool sync_old_done;
	time_t until_timestamp;
	int mark_fd;
	int binlog_index;
	int binlog_fd;
	off_t binlog_offset;
	int64_t scan_row_count;
	int64_t sync_row_count;

	int64_t last_scan_rows;  //for write to mark file
	int64_t last_sync_rows;  //for write to mark file
} BinLogReader;

typedef struct
{
	time_t timestamp;
	char op_type;
	int key_hash_code;  //key hash code
	FDHTKeyInfo key_info;
	BinField value;
	time_t expires;  //key expires, 0 for never expired
} BinLogRecord;

extern int g_binlog_fd;
extern int g_binlog_index;
extern off_t g_binlog_file_size;

extern int g_fdht_sync_thread_count;

int fdht_sync_init();
int fdht_sync_destroy();
int fdht_binlog_write(const time_t timestamp, const char op_type, \
		const int key_hash_code, const time_t expires, \
		FDHTKeyInfo *pKeyInfo, const char *pValue, const int value_len);

int fdht_binlog_read(BinLogReader *pReader, \
		BinLogRecord *pRecord, int *record_length);
int fdht_open_readable_binlog(BinLogReader *pReader);

int fdht_binlog_sync_func(void *args);
int write_to_sync_ini_file();
int kill_fdht_sync_threads();

#ifdef __cplusplus
}
#endif

#endif
