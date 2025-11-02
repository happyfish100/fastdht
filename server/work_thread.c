/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.fastken.com/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_service.h"
#include "sf/sf_nio.h"
#include "fdht_define.h"
#include "fdht_global.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "global.h"
#include "func.h"
#include "store.h"
#include "key_op.h"
#include "sync.h"
#include "mpool_op.h"
#include "work_thread.h"

#define SYNC_REQ_WAIT_SECONDS	60

static pthread_mutex_t inc_thread_mutex;
static time_t first_sync_req_time = 0;

static int work_deal_task(struct fast_task_info *pTask, const int stage);

static int deal_cmd_get(struct fast_task_info *pTask);
static int deal_cmd_set(struct fast_task_info *pTask, byte op_type);
static int deal_cmd_del(struct fast_task_info *pTask, byte op_type);
static int deal_cmd_inc(struct fast_task_info *pTask);
static int deal_cmd_sync_req(struct fast_task_info *pTask);
static int deal_cmd_sync_done(struct fast_task_info *pTask);
static int deal_cmd_batch_get(struct fast_task_info *pTask);
static int deal_cmd_batch_set(struct fast_task_info *pTask);
static int deal_cmd_batch_del(struct fast_task_info *pTask);
static int deal_cmd_stat(struct fast_task_info *pTask);
static int deal_cmd_get_sub_keys(struct fast_task_info *pTask);

static int sock_accept_done_callback(struct fast_task_info *task,
        const in_addr_64_t client_addr, const bool bInnerPort)
{
    if (g_allow_ip_count >= 0)
    {
        if (bsearch(&client_addr, g_allow_ip_addrs,
                    g_allow_ip_count, sizeof(in_addr_64_t),
                    cmp_by_ip_addr_t) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "ip addr %s is not allowed to access",
                    __LINE__, task->client_ip);
            return EPERM;
        }
    }

    return 0;
}

static int fdht_set_body_length(struct fast_task_info *pTask)
{
    pTask->recv.ptr->length = buff2int(((FDHTProtoHeader *)
                pTask->recv.ptr->data)->pkg_len);
    if (pTask->recv.ptr->length < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, pkg length: %d < 0", __LINE__,
                pTask->client_ip, pTask->recv.ptr->length);
        return EINVAL;
    }

    return 0;
}

int work_thread_init()
{
    int result;

    if ((result=init_pthread_lock(&inc_thread_mutex)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, program exit!", __LINE__);
        return result;
    }

    result = sf_service_init("service", NULL, NULL,
            sock_accept_done_callback, fdht_set_body_length, NULL,
            work_deal_task, sf_task_finish_clean_up, NULL, 1000,
            sizeof(FDHTProtoHeader), 0);
    sf_enable_thread_notify(false);

    return 0;
}

void work_thread_destroy()
{
	pthread_mutex_destroy(&inc_thread_mutex);
}

static int work_deal_task(struct fast_task_info *pTask, const int stage)
{
	FDHTProtoHeader *pHeader;
	int result;

	switch(((FDHTProtoHeader *)pTask->send.ptr->data)->cmd)
	{
		case FDHT_PROTO_CMD_GET:
			result = deal_cmd_get(pTask);
			break;
		case FDHT_PROTO_CMD_SET:
			result = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_SOURCE_SET);
			break;
		case FDHT_PROTO_CMD_SYNC_SET:
			result = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_REPLICA_SET);
			break;
		case FDHT_PROTO_CMD_INC:
			result = deal_cmd_inc(pTask);
			break;
		case FDHT_PROTO_CMD_DEL:
			result = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_SOURCE_DEL);
			break;
		case FDHT_PROTO_CMD_SYNC_DEL:
			result = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_REPLICA_DEL);
			break;
		case FDHT_PROTO_CMD_HEART_BEAT:
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			result = 0;
			break;
		case FDHT_PROTO_CMD_QUIT:
			sf_task_finish_clean_up(pTask);
			return 0;
		case FDHT_PROTO_CMD_BATCH_GET:
			result = deal_cmd_batch_get(pTask);
			break;
		case FDHT_PROTO_CMD_BATCH_SET:
			result = deal_cmd_batch_set(pTask);
			break;
		case FDHT_PROTO_CMD_BATCH_DEL:
			result = deal_cmd_batch_del(pTask);
			break;
		case FDHT_PROTO_CMD_SYNC_REQ:
			result = deal_cmd_sync_req(pTask);
			break;
		case FDHT_PROTO_CMD_SYNC_NOTIFY:
			result = deal_cmd_sync_done(pTask);
			break;
		case FDHT_PROTO_CMD_STAT:
			result = deal_cmd_stat(pTask);
			break;
		case FDHT_PROTO_CMD_GET_SUB_KEYS:
			result = deal_cmd_get_sub_keys(pTask);
			break;
		default:
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid cmd: 0x%02X", \
				__LINE__, pTask->client_ip, \
				((FDHTProtoHeader *)pTask->send.ptr->data)->cmd);

			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			result = EINVAL;
			break;
	}

	pHeader = (FDHTProtoHeader *)pTask->send.ptr->data;
	pHeader->status = result;
	int2buff((int)g_current_time, pHeader->timestamp);
	pHeader->cmd = FDHT_PROTO_CMD_RESP;
	int2buff(pTask->send.ptr->length - sizeof(FDHTProtoHeader), pHeader->pkg_len);

	sf_send_add_event(pTask);
	return 0;
}

#define CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires) \
	key_hash_code = buff2int(((FDHTProtoHeader *)pTask->send.ptr->data)->key_hash_code); \
	timestamp = buff2int(((FDHTProtoHeader *)pTask->send.ptr->data)->timestamp); \
	new_expires = buff2int(((FDHTProtoHeader *)pTask->send.ptr->data)->expires); \
	if (timestamp > 0) \
	{ \
		if (new_expires > 0)  \
		{ \
			new_expires = g_current_time + (new_expires - timestamp); \
		} \
	} \
	group_id = ((unsigned int)key_hash_code) % g_group_count; \
	if (group_id >= g_db_count) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_id: %d, " \
			"which < 0 or >= %d", \
			__LINE__, pTask->client_ip, group_id, g_db_count); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return  EINVAL; \
	} \
	if (g_db_list[group_id] == NULL) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_id: %d, " \
			"which does not belong to this server", \
			__LINE__, pTask->client_ip, group_id); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return  EINVAL; \
	} \

#define PARSE_COMMON_BODY_BEFORE_KEY(min_body_len, pTask, nInBodyLen, key_info,\
		pNameSpace, pObjectId) \
	nInBodyLen = pTask->send.ptr->length - sizeof(FDHTProtoHeader); \
	if (nInBodyLen <= min_body_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, nInBodyLen, min_body_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
 \
	key_info.namespace_len = buff2int(pTask->send.ptr->data + sizeof(FDHTProtoHeader)); \
	if (key_info.namespace_len < 0 || \
		key_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid namespace length: %d", \
			__LINE__, pTask->client_ip, key_info.namespace_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen <= min_body_len + key_info.namespace_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, min_body_len + key_info.namespace_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	pNameSpace = pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 4; \
	if (key_info.namespace_len > 0) \
	{ \
		memcpy(key_info.szNameSpace,pNameSpace,key_info.namespace_len);\
	} \
 \
	key_info.obj_id_len = buff2int(pNameSpace + key_info.namespace_len); \
	if (key_info.obj_id_len < 0 || \
		key_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid object length: %d", \
			__LINE__, pTask->client_ip, key_info.obj_id_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen <= min_body_len + key_info.namespace_len + \
			key_info.obj_id_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			min_body_len + key_info.namespace_len + \
			key_info.obj_id_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	pObjectId = pNameSpace + key_info.namespace_len + 4; \
	if (key_info.obj_id_len > 0) \
	{ \
		memcpy(key_info.szObjectId, pObjectId, key_info.obj_id_len); \
	} \


#define PARSE_COMMON_BODY_KEY(min_body_len, pTask, nInBodyLen, key_info, \
		pNameSpace, pObjectId, pKey) \
	key_info.key_len = buff2int(pObjectId + key_info.obj_id_len); \
	if (key_info.key_len < 0 || key_info.key_len > FDHT_MAX_SUB_KEY_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid key length: %d", \
			__LINE__, pTask->client_ip, key_info.key_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen < min_body_len + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, min_body_len + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	} \
	pKey = pObjectId + key_info.obj_id_len + 4; \
	memcpy(key_info.szKey, pKey, key_info.key_len); \

#define CHECK_SUB_KEY_NAME(key_info) \
	if (key_info.key_len == FDHT_LIST_KEY_NAME_LEN && memcmp( \
		key_info.szKey, FDHT_LIST_KEY_NAME_STR, \
		FDHT_LIST_KEY_NAME_LEN) == 0) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid sub key name: %s", \
			__LINE__, pTask->client_ip, FDHT_LIST_KEY_NAME_STR); \
		pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
		return EINVAL; \
	}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*       value_len:  4 bytes big endian integer
*       value:      value buff
*/
static int deal_cmd_get(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int old_expires;
	int new_expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char *pValue;
	char *p;  //tmp var
	int full_key_len;
	int value_len;
	int result;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(12, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)
	PARSE_COMMON_BODY_KEY(12, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	if (nInBodyLen != 12 + key_info.namespace_len + key_info.obj_id_len + \
				key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 12 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	CHECK_SUB_KEY_NAME(key_info)
	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	pValue = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
	value_len = pTask->send.ptr->size - sizeof(FDHTProtoHeader);
	if ((result=g_func_get(g_db_list[group_id], full_key, full_key_len, \
               	&pValue, &value_len)) != 0)
	{
		if (result == ENOSPC)
		{
			char *pTemp;

			pTemp = (char *)pTask->send.ptr->data;
			pTask->send.ptr->data = malloc(sizeof(FDHTProtoHeader) + value_len);
			if (pTask->send.ptr->data == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->send.ptr->size, \
					errno, STRERROR(errno));

				pTask->send.ptr->data = pTemp;  //restore old data
				pTask->send.ptr->length = sizeof(FDHTProtoHeader);
				return ENOMEM;
			}

			memcpy(pTask->send.ptr->data, pTemp, sizeof(FDHTProtoHeader));
			free(pTemp);
			pTask->send.ptr->size = sizeof(FDHTProtoHeader) + value_len;

			pValue = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
			if ((result=g_func_get(g_db_list[group_id], full_key, \
				full_key_len, &pValue, &value_len)) != 0)
			{
				pTask->send.ptr->length = sizeof(FDHTProtoHeader);
				return result;
			}
		}
		else
		{
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return result;
		}
	}

	old_expires = buff2int(pValue);
	if (old_expires != FDHT_EXPIRES_NEVER && old_expires < g_current_time)
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return ENOENT;
	}

	if (new_expires != FDHT_EXPIRES_NONE)
	{
		int2buff(new_expires, pValue);
		result = g_func_partial_set(g_db_list[group_id], full_key, \
			full_key_len, pValue, 0, 4);
	}

	value_len -= 4;
	pTask->send.ptr->length = sizeof(FDHTProtoHeader) + 4 + value_len;
	memcpy(((FDHTProtoHeader *)pTask->send.ptr->data)->expires, pValue, 4);
	int2buff(value_len, pTask->send.ptr->data+sizeof(FDHTProtoHeader));

	return 0;
}


#define CHECK_BUFF_SIZE(pTask, old_len, value_len, new_size, pTemp) \
			new_size = old_len + value_len + 8 * 1024; \
			pTemp = (char *)pTask->send.ptr->data; \
			pTask->send.ptr->data = realloc(pTask->send.ptr->data, new_size); \
			if (pTask->send.ptr->data == NULL) \
			{ \
				logError("file: "__FILE__", line: %d, " \
					"realloc %d bytes failed, " \
					"errno: %d, error info: %s", \
					__LINE__, new_size, \
					errno, STRERROR(errno)); \
 \
				pTask->send.ptr->data = pTemp;  /* restore old data */ \
				pTask->send.ptr->length = sizeof(FDHTProtoHeader); \
				return ENOMEM; \
			} \
 \
			pTask->send.ptr->size = new_size; \

static int compare_sub_key(const void *p1, const void *p2)
{
	return strcmp(((FDHTSubKey *)p1)->szKey, ((FDHTSubKey *)p2)->szKey);
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_count: 4 bytes key count (big endian integer), must > 0
*       key_len*:  4 bytes big endian integer
*       key*:      key name
*       value_len*:  4 bytes big endian integer
*       value*:      value_len bytes value buff
* response body format:
*       key_count: key count, 4 bytes big endian integer
*       success_count: success key count, 4 bytes big endian integer
*       key_len*:  4 bytes big endian integer
*       key*:      key_len bytes key name
*       status*:     1 byte key status
*/
static int deal_cmd_batch_set(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int new_expires;
	char *pNameSpace;
	int key_count;
	int success_count;
	int i;
	int common_fileds_len;
	char *pObjectId;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char *pSrcStart;
	char *pSrc;
	char *pDest;
	char *p;  //tmp var
	int full_key_len;
	int value_len;
	int result;
	FDHTSubKey subKeys[FDHT_MAX_KEY_COUNT_PER_REQ];

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(20, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)

	key_count = buff2int(pObjectId + key_info.obj_id_len);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid key count: %d", \
			__LINE__, pTask->client_ip, key_count);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	common_fileds_len = 12 + key_info.namespace_len + key_info.obj_id_len;
	
	success_count = 0;
	result = 0;

	timestamp = g_current_time;
	pSrc = pSrcStart = pObjectId + key_info.obj_id_len + 4;
	pDest = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
	int2buff(key_count, pDest);
	pDest += 8;
	for (i=0; i<key_count; i++)
	{
		key_info.key_len = buff2int(pSrc);
		if (key_info.key_len <= 0 || \
			key_info.key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid key length: %d", \
				__LINE__, pTask->client_ip, key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}

		if (nInBodyLen < common_fileds_len + (pSrc - pSrcStart) + \
				8 + key_info.key_len)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, body length: %d != %d", \
				__LINE__, pTask->client_ip, nInBodyLen, \
				common_fileds_len + (int)(pSrc - pSrcStart) \
				+ 8 + key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}
		memcpy(key_info.szKey, pSrc + 4, key_info.key_len);
		pSrc += 4 + key_info.key_len;

		CHECK_SUB_KEY_NAME(key_info)

		value_len = buff2int(pSrc);
		if (nInBodyLen < common_fileds_len + (pSrc - pSrcStart) + \
				4 + value_len)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, body length: %d != %d", \
				__LINE__, pTask->client_ip, nInBodyLen, \
				common_fileds_len + (int)(pSrc - pSrcStart) \
				+ 4 + value_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}

		int2buff(key_info.key_len, pDest);
		pDest += 4;
		memcpy(pDest, key_info.szKey, key_info.key_len);
		pDest += key_info.key_len;

		FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

		value_len += 4; //including expires field
		int2buff(new_expires, pSrc);
		*pDest++ = result = g_func_set(g_db_list[group_id], full_key, \
				full_key_len, pSrc, value_len);
		if (result == 0)
		{
			if (g_write_to_binlog_flag)
			{
				fdht_binlog_write(timestamp, \
					FDHT_OP_TYPE_SOURCE_SET, \
					key_hash_code, new_expires, &key_info, \
					pSrc + 4, value_len - 4);
			}

			if (g_store_sub_keys)
			{
				subKeys[success_count].key_len = \
						key_info.key_len;
				memcpy(subKeys[success_count].szKey, \
					key_info.szKey, key_info.key_len);
				*(subKeys[success_count].szKey + \
					key_info.key_len) = '\0';
			}

			success_count++;
		}

		pSrc += value_len;
	}

	if (nInBodyLen != common_fileds_len + (pSrc - pSrcStart))
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			common_fileds_len + (int)(pSrc - pSrcStart));
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	if (success_count > 0)
	{
		if (g_store_sub_keys)
		{
			if (success_count > 1)
			{
				qsort(subKeys, success_count, \
					sizeof(FDHTSubKey), compare_sub_key);
			}
			key_batch_add(g_db_list[group_id], &key_info, \
				key_hash_code, subKeys, success_count);
		}

		int2buff(success_count, pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 4);
		pTask->send.ptr->length = pDest - pTask->send.ptr->data;
		int2buff(new_expires, ((FDHTProtoHeader *)pTask->send.ptr->data)->expires);
		return 0;
	}
	else
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return result;
	}
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_count: 4 bytes key count (big endian integer), must > 0
*       key_len*:  4 bytes big endian integer
*       key*:      key name
* response body format:
*       key_count: key count, 4 bytes big endian integer
*       success_count: success key count, 4 bytes big endian integer
*       key_len*:  4 bytes big endian integer
*       key*:      key_len bytes key name
*       status*:     1 byte key status
*       value_len*:  4 bytes big endian integer (when status == 0)
*       value*:      value_len bytes value buff (when status == 0)
*/
static int deal_cmd_batch_get(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int old_expires;
	int new_expires;
	int min_expires;
	char *pNameSpace;
	int key_count;
	int success_count;
	int i;
	int common_fileds_len;
	char *pObjectId;
	char in_buff[(4 + FDHT_MAX_SUB_KEY_LEN) * FDHT_MAX_KEY_COUNT_PER_REQ];
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char szExpired[4];
	char *pValue;
	char *pSrc;
	char *pDest;
	char *p;  //tmp var
	int full_key_len;
	int value_len;
	time_t current_time;
	int result;
	char *pTemp;
	int old_len;
	int new_size;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(16, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)

	key_count = buff2int(pObjectId + key_info.obj_id_len);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid key count: %d", \
			__LINE__, pTask->client_ip, key_count);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	common_fileds_len = 12 + key_info.namespace_len + key_info.obj_id_len;
	if (nInBodyLen > common_fileds_len + (4 + FDHT_MAX_SUB_KEY_LEN) * key_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d > %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			common_fileds_len + (4 + FDHT_MAX_SUB_KEY_LEN) * key_count);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}
	
	if (new_expires != FDHT_EXPIRES_NONE)
	{
		min_expires = new_expires;
		int2buff(new_expires, szExpired);
	}
	else
	{
		min_expires = FDHT_EXPIRES_NEVER;
		memset(szExpired, 0, sizeof(szExpired));
	}

	success_count = 0;
	result = 0;
	current_time = g_current_time;

	memcpy(in_buff, pObjectId + key_info.obj_id_len + 4, \
		nInBodyLen - common_fileds_len);
	pSrc = in_buff;

	pDest = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
	int2buff(key_count, pDest);
	pDest += 8;
	for (i=0; i<key_count; i++)
	{
		key_info.key_len = buff2int(pSrc);
		if (key_info.key_len <= 0 || \
			key_info.key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid key length: %d", \
				__LINE__, pTask->client_ip, key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}

		if (nInBodyLen < common_fileds_len + (pSrc - in_buff) + \
				4 + key_info.key_len)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, body length: %d != %d", \
				__LINE__, pTask->client_ip, nInBodyLen, \
				common_fileds_len + (int)(pSrc - in_buff) + \
				4 + key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}
		memcpy(key_info.szKey, pSrc + 4, key_info.key_len);
		pSrc += 4 + key_info.key_len;

		CHECK_SUB_KEY_NAME(key_info)

		old_len = pDest - pTask->send.ptr->data;
		value_len = 9 + key_info.key_len;
		if (pTask->send.ptr->size <= old_len + value_len)
		{
			CHECK_BUFF_SIZE(pTask, old_len, value_len, \
					new_size, pTemp)
			pDest = pTask->send.ptr->data + old_len;
		}

		int2buff(key_info.key_len, pDest);
		pDest += 4;
		memcpy(pDest, key_info.szKey, key_info.key_len);
		pDest += key_info.key_len + 1;

		FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

		pValue = pDest;
		value_len = pTask->send.ptr->size - (pDest - pTask->send.ptr->data);
		result = g_func_get(g_db_list[group_id], full_key, full_key_len, \
				&pValue, &value_len);
		if (result != 0)
		{
			if (result == ENOSPC)
			{
				old_len = pDest - pTask->send.ptr->data;

				CHECK_BUFF_SIZE(pTask, old_len, value_len, \
						new_size, pTemp)

				pDest = pTask->send.ptr->data + old_len;

				pValue = pDest;
				if ((result=g_func_get(g_db_list[group_id], \
						full_key, full_key_len, \
						&pValue, &value_len)) != 0)
				{
					*(pDest-1) = result;
					continue;
				}
			}
			else
			{
				*(pDest-1) = result;
				continue;
			}
		}

		old_expires = buff2int(pValue);
		if (old_expires != FDHT_EXPIRES_NEVER && \
			old_expires < current_time)
		{
			*(pDest-1) = result = ENOENT;
			continue;
		}

		if (new_expires != FDHT_EXPIRES_NONE)
		{
			if ((result = g_func_partial_set(g_db_list[group_id], \
				full_key, full_key_len, szExpired, 0, 4)) != 0)
			{
				*(pDest-1) = result;
				continue;
			}
		}
		else
		{
			if (min_expires == FDHT_EXPIRES_NEVER)
			{
				if (old_expires != FDHT_EXPIRES_NEVER)
				{
					min_expires = old_expires;
				}
			}
			else
			{
				if (old_expires != FDHT_EXPIRES_NEVER && \
						old_expires < min_expires)
				{
					min_expires = old_expires;
				}
			}
		}

		success_count++;
		*(pDest-1) = 0;
		int2buff(value_len - 4, pDest);
		pDest += value_len;
	}

	if (nInBodyLen != common_fileds_len + (pSrc - in_buff))
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			common_fileds_len + (int)(pSrc - in_buff));
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	if (success_count > 0)
	{
		int2buff(success_count, pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 4);
		pTask->send.ptr->length = pDest - pTask->send.ptr->data;
		int2buff(min_expires, ((FDHTProtoHeader *)pTask->send.ptr->data)->expires);
		return 0;
	}
	else
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return result;
	}
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
* response body format:
*       sub key list: FDHT_FULL_KEY_SEPERATOR split sub keys
*/
static int deal_cmd_get_sub_keys(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int new_expires;
	char *pNameSpace;
	char *pObjectId;
	char *key_list;
	int result;
	int keys_len;
	char saved_keep_alive;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(7, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)
	if (nInBodyLen != 8 + key_info.namespace_len + key_info.obj_id_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			8 + key_info.namespace_len + key_info.obj_id_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	key_info.key_len = FDHT_LIST_KEY_NAME_LEN;
	memcpy(key_info.szKey, FDHT_LIST_KEY_NAME_STR, FDHT_LIST_KEY_NAME_LEN);

	saved_keep_alive = ((FDHTProtoHeader *)(pTask->send.ptr->data))->keep_alive;
	keys_len = pTask->send.ptr->size - sizeof(FDHTProtoHeader) + 4;
	key_list = pTask->send.ptr->data + sizeof(FDHTProtoHeader) - 4;
	result = key_get(g_db_list[group_id], &key_info, \
                	key_list, &keys_len);
	((FDHTProtoHeader *)(pTask->send.ptr->data))->keep_alive = saved_keep_alive;
	if (result == 0)
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader) + (keys_len - 4);
		return 0;
	}
	else
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return result;
	}
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_count: 4 bytes key count (big endian integer), must > 0
*       key_len*:  4 bytes big endian integer
*       key*:      key name
* response body format:
*       key_count: key count, 4 bytes big endian integer
*       success_count: success key count, 4 bytes big endian integer
*       key_len*:  4 bytes big endian integer
*       key*:      key_len bytes key name
*       status*:     1 byte key status
*/
static int deal_cmd_batch_del(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int new_expires;
	char *pNameSpace;
	int key_count;
	int success_count;
	int i;
	int common_fileds_len;
	char *pObjectId;
	char in_buff[(4 + FDHT_MAX_SUB_KEY_LEN) * FDHT_MAX_KEY_COUNT_PER_REQ];
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char *pSrc;
	char *pDest;
	char *p;  //tmp var
	int full_key_len;
	int result;
	FDHTSubKey subKeys[FDHT_MAX_KEY_COUNT_PER_REQ];

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(16, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)

	key_count = buff2int(pObjectId + key_info.obj_id_len);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid key count: %d", \
			__LINE__, pTask->client_ip, key_count);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	common_fileds_len = 12 + key_info.namespace_len + key_info.obj_id_len;
	if (nInBodyLen > common_fileds_len + (4 + FDHT_MAX_SUB_KEY_LEN) \
			* key_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d > %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			common_fileds_len + (4 + FDHT_MAX_SUB_KEY_LEN) * \
			key_count);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	timestamp = g_current_time;
	success_count = 0;
	result = 0;

	memcpy(in_buff, pObjectId + key_info.obj_id_len + 4, \
		nInBodyLen - common_fileds_len);
	pSrc = in_buff;

	pDest = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
	int2buff(key_count, pDest);
	pDest += 8;
	for (i=0; i<key_count; i++)
	{
		key_info.key_len = buff2int(pSrc);
		if (key_info.key_len <= 0 || \
			key_info.key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid key length: %d", \
				__LINE__, pTask->client_ip, key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}

		if (nInBodyLen < common_fileds_len + (pSrc - in_buff) + \
				4 + key_info.key_len)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, body length: %d != %d", \
				__LINE__, pTask->client_ip, nInBodyLen, \
				common_fileds_len + (int)(pSrc - in_buff) + \
				4 + key_info.key_len);
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EINVAL;
		}
		memcpy(key_info.szKey, pSrc + 4, key_info.key_len);
		pSrc += 4 + key_info.key_len;

		CHECK_SUB_KEY_NAME(key_info)

		int2buff(key_info.key_len, pDest);
		pDest += 4;
		memcpy(pDest, key_info.szKey, key_info.key_len);
		pDest += key_info.key_len;

		FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

		*pDest++ = result = g_func_delete(g_db_list[group_id], \
				full_key, full_key_len);
		if (result == 0)
		{
			if (g_write_to_binlog_flag)
			{
				fdht_binlog_write(timestamp, \
					FDHT_OP_TYPE_SOURCE_DEL, \
					key_hash_code, FDHT_EXPIRES_NEVER, \
					&key_info, NULL, 0);
			}

			if (g_store_sub_keys)
			{
				subKeys[success_count].key_len = \
						key_info.key_len;
				memcpy(subKeys[success_count].szKey, \
					key_info.szKey, key_info.key_len);
				*(subKeys[success_count].szKey + \
					key_info.key_len) = '\0';
			}

			success_count++;
		}
	}

	if (nInBodyLen != common_fileds_len + (pSrc - in_buff))
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			common_fileds_len + (int)(pSrc - in_buff));
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	if (success_count > 0)
	{
		if (g_store_sub_keys)
		{
			if (success_count > 1)
			{
				qsort(subKeys, success_count, \
					sizeof(FDHTSubKey), compare_sub_key);
			}

			key_batch_del(g_db_list[group_id], &key_info, \
				key_hash_code, subKeys, success_count);
		}

		int2buff(success_count, pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 4);
		pTask->send.ptr->length = pDest - pTask->send.ptr->data;
		return 0;
	}
	else
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return result;
	}
}

#define PACK_SYNC_REQ_BODY(pTask) \
	pTask->send.ptr->length = sizeof(FDHTProtoHeader) + 1 + IP_ADDRESS_SIZE + 8; \
	*(pTask->send.ptr->data + sizeof(FDHTProtoHeader)) = g_sync_old_done; \
	memcpy(pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 1, \
		g_sync_src_ip_addr, IP_ADDRESS_SIZE); \
	int2buff(g_sync_src_port, pTask->send.ptr->data + \
		sizeof(FDHTProtoHeader) + 1 + IP_ADDRESS_SIZE); \
	int2buff(g_sync_until_timestamp, pTask->send.ptr->data + \
		sizeof(FDHTProtoHeader) + 1 + IP_ADDRESS_SIZE + 4);

/**
* request body format:
*      server port : 4 bytes
*      sync_old_done: 1 byte
*      update count: 8 bytes
* response body format:
*      sync_old_done: 1 byte
*      sync_src_ip_addr: IP_ADDRESS_SIZE bytes
*      sync_src_port:  4 bytes
*      sync_until_timestamp: 4 bytes
*/
static int deal_cmd_sync_req(struct fast_task_info *pTask)
{
	int result;
	int nInBodyLen;
	int64_t update_count;
	FDHTGroupServer targetServer;
	FDHTGroupServer *pFound;
	FDHTGroupServer *pServer;
	FDHTGroupServer *pEnd;
	FDHTGroupServer *pFirstServer;
	FDHTGroupServer *pMaxCountServer;
	bool src_sync_old_done;

	nInBodyLen = pTask->send.ptr->length - sizeof(FDHTProtoHeader);
	if (nInBodyLen != 13)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != 13", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	if (g_sync_old_done)
	{
		PACK_SYNC_REQ_BODY(pTask)
		return 0;
	}

	memset(&targetServer, 0, sizeof(FDHTGroupServer));
	strcpy(targetServer.ip_addr, pTask->client_ip);
	targetServer.port = buff2int(pTask->send.ptr->data + sizeof(FDHTProtoHeader));
	src_sync_old_done = *(pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 4);
	update_count = buff2long(pTask->send.ptr->data + sizeof(FDHTProtoHeader) + 5);

	pFound = (FDHTGroupServer *)bsearch(&targetServer, \
			g_group_servers, g_group_server_count, \
			sizeof(FDHTGroupServer),group_cmp_by_ip_and_port);
	if (pFound == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%u not in my group!", \
			__LINE__, pTask->client_ip, targetServer.port);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);

		if (g_log_context.log_level >= LOG_DEBUG)
		{
			int k;
			logDebug("My group server list:");
			for (k=0; k<g_group_server_count; k++)
			{
				logDebug("\t%d. %s:%u", k+1, \
					g_group_servers[k].ip_addr, \
					g_group_servers[k].port);
			}
		}

		return ENOENT;
	}

	if (first_sync_req_time == 0)
	{
		first_sync_req_time = g_current_time;
	}

	pFound->sync_old_done = src_sync_old_done;
	pFound->sync_req_count++;
	pFound->update_count = update_count;

	pEnd = g_group_servers + g_group_server_count;
	pFirstServer = g_group_servers;
	while (pFirstServer < pEnd && is_local_host_ip(pFirstServer->ip_addr))
	{
		pFirstServer++;
	}

	if (pFirstServer == pEnd) //impossible
	{
		logError("file: "__FILE__", line: %d, " \
			"client: %s:%u, the ip addresses of all servers " \
			"are local ip addresses.", __LINE__, \
			pTask->client_ip, targetServer.port);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return ENOENT;
	}

	while (1)
	{
		if (pFirstServer->sync_req_count > 0 && pFirstServer->sync_old_done)
		{
			pServer = pFirstServer;
			break;
		}

		pServer = pFirstServer;
		while (pServer < pEnd)
		{
			if (is_local_host_ip(pServer->ip_addr))
			{
				pServer++;
				continue;
			}

			if (pServer->sync_req_count == 0)
			{
				break;
			}

			if (pServer->sync_old_done)
			{
				break;
			}

			pServer++;
		}

		if (pServer >= pEnd) //all is new server?
		{
			pMaxCountServer = pFirstServer;
			pServer = pFirstServer + 1;
			while (pServer < pEnd)
			{
				if (is_local_host_ip(pServer->ip_addr))
				{
					pServer++;
					continue;
				}

				if (pServer->update_count > pMaxCountServer->update_count)
				{
					pMaxCountServer = pServer;
				}

				pServer++;
			}

			pServer = pMaxCountServer;
			break;
		}

		if (g_current_time - first_sync_req_time < SYNC_REQ_WAIT_SECONDS)
		{
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return EAGAIN;
		}

		pServer = pFirstServer + 1;
		while (pServer < pEnd)
		{
			if (pServer->sync_req_count > 0 && \
			     pServer->sync_old_done && \
			     !is_local_host_ip(pServer->ip_addr))
			{
				break;
			}

			pServer++;
		}

		if (pServer >= pEnd)
		{
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return ENOENT;
		}

		break;
	}

	if (!(strcmp(pTask->client_ip, pServer->ip_addr) == 0 && \
		targetServer.port == pServer->port))
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EAGAIN;
	}

	if (pServer->update_count > 0)
	{
		strcpy(g_sync_src_ip_addr, pServer->ip_addr);
		g_sync_src_port = pServer->port;
		g_sync_until_timestamp = g_current_time;
	}
	else
	{
		g_sync_old_done = true;  //no old data to sync
	}

	if ((result=write_to_sync_ini_file()) != 0)
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return result;
	}

	PACK_SYNC_REQ_BODY(pTask)
	return 0;
}

static int deal_cmd_sync_done(struct fast_task_info *pTask)
{
	int result;
	int nInBodyLen;
	int src_port;

	nInBodyLen = pTask->send.ptr->length - sizeof(FDHTProtoHeader);
	if (nInBodyLen != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != 4", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	pTask->send.ptr->length = sizeof(FDHTProtoHeader);
	src_port = buff2int(pTask->send.ptr->data + sizeof(FDHTProtoHeader));
	if (!(strcmp(pTask->client_ip, g_sync_src_ip_addr) == 0 && \
		src_port == g_sync_src_port))
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%u not the sync src server!", \
			__LINE__, pTask->client_ip, src_port);
		return EINVAL;
	}

	if (g_sync_old_done)
	{
		return 0;
	}

	g_sync_old_done = true;
	g_sync_done_timestamp = g_current_time;
	if ((result=write_to_sync_ini_file()) != 0)
	{
		return result;
	}

	return 0;
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
*       value_len:  4 bytes big endian integer
*       value:      value buff
* response body format:
*      none
*/
static int deal_cmd_set(struct fast_task_info *pTask, byte op_type)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int group_id;
	int key_hash_code;
	time_t timestamp;
	time_t new_expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	char *p;  //tmp var
	char *pValue;
	int value_len;
	int result;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(16, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)
	PARSE_COMMON_BODY_KEY(16, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId, pKey)

	value_len = buff2int(pKey + key_info.key_len);
	if (value_len < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, value length: %d < 0", \
			__LINE__, pTask->client_ip, value_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return  EINVAL;
	}
	if (nInBodyLen != 16 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len + value_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 16 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len + value_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return  EINVAL;
	}

	if (op_type == FDHT_OP_TYPE_SOURCE_SET)
	{
		CHECK_SUB_KEY_NAME(key_info)
	}

	pValue = pKey + key_info.key_len;

	int2buff(new_expires, pValue);
	value_len += 4;

	pTask->send.ptr->length = sizeof(FDHTProtoHeader);

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	result = g_func_set(g_db_list[group_id], full_key, full_key_len, \
			pValue, value_len);
	if (result == 0)
	{
		memcpy(((FDHTProtoHeader *)pTask->send.ptr->data)->expires, pValue, 4);

		if (g_write_to_binlog_flag)
		{
			if (op_type == FDHT_OP_TYPE_SOURCE_SET)
			{
				timestamp = g_current_time;
			}

			fdht_binlog_write(timestamp, op_type, key_hash_code, \
				new_expires, &key_info, pValue+4, value_len-4);
		}

		if (g_store_sub_keys && op_type == FDHT_OP_TYPE_SOURCE_SET)
		{
			key_add(g_db_list[group_id], &key_info, key_hash_code);
		}
	}

	return result;
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*      none
*/
static int deal_cmd_del(struct fast_task_info *pTask, byte op_type)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int new_expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	int result;
	char *p;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(12, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)
	PARSE_COMMON_BODY_KEY(12, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	if (nInBodyLen != 12 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 12 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return  EINVAL;
	}

	if (op_type == FDHT_OP_TYPE_SOURCE_DEL)
	{
		CHECK_SUB_KEY_NAME(key_info)
	}

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	pTask->send.ptr->length = sizeof(FDHTProtoHeader);
	result = g_func_delete(g_db_list[group_id], full_key, full_key_len);
	if (result == 0)
	{
		if (g_write_to_binlog_flag)
		{
			if (op_type == FDHT_OP_TYPE_SOURCE_DEL)
			{
				timestamp = g_current_time;
			}
			fdht_binlog_write(timestamp, op_type, key_hash_code, \
				FDHT_EXPIRES_NEVER, &key_info, NULL, 0);
		}

		if (g_store_sub_keys && op_type == FDHT_OP_TYPE_SOURCE_DEL)
		{
			key_del(g_db_list[group_id], &key_info, key_hash_code);
		}
	}

	return result;
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
*       incr      4 bytes big endian integer
* response body format:
*      value_len: 4 bytes big endian integer
*      value :  value_len bytes
*/
static int deal_cmd_inc(struct fast_task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	time_t new_expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char value[32];
	int full_key_len;
	int value_len;
	int inc;
	char *p;  //tmp var
	int result;
	int lock_res;

	memset(&key_info, 0, sizeof(key_info));
	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, new_expires)

	PARSE_COMMON_BODY_BEFORE_KEY(16, pTask, nInBodyLen, key_info, \
			pNameSpace, pObjectId)
	PARSE_COMMON_BODY_KEY(16, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	if (nInBodyLen != 16 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 16 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return  EINVAL;
	}

	CHECK_SUB_KEY_NAME(key_info)

	inc = buff2int(pKey + key_info.key_len);

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	if (SF_G_WORK_THREADS > 1 && (lock_res=pthread_mutex_lock( \
			&inc_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, lock_res, STRERROR(lock_res));
	}

	value_len = sizeof(value) - 1;
	result = g_func_inc_ex(g_db_list[group_id], full_key, full_key_len, inc, \
			value, &value_len, new_expires);

	if (SF_G_WORK_THREADS > 1 && (lock_res=pthread_mutex_unlock(
			&inc_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, lock_res, STRERROR(lock_res));
	}

	if (result == 0)
	{
		value_len -= 4;  //skip expires
		if (g_write_to_binlog_flag)
		{
			new_expires = (time_t)buff2int(value);
			fdht_binlog_write(g_current_time, FDHT_OP_TYPE_SOURCE_SET, \
				key_hash_code, new_expires, &key_info, \
				value+4, value_len);
		}

		pTask->send.ptr->length = sizeof(FDHTProtoHeader) + 4 + value_len;
		int2buff(value_len, pTask->send.ptr->data + sizeof(FDHTProtoHeader));
		memcpy(((FDHTProtoHeader *)pTask->send.ptr->data)->expires, value, 4);
		memcpy(pTask->send.ptr->data+sizeof(FDHTProtoHeader)+4, value+4, value_len);

		if (g_store_sub_keys)
		{
			key_add(g_db_list[group_id], &key_info, key_hash_code);
		}
	}
	else
	{
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
	}

	return result;
}

/**
* request body format:
*      none
* response body format:
*      key value pair: key=value, row seperate by new line (\n)
*/
static int deal_cmd_stat(struct fast_task_info *pTask)
{
	int nInBodyLen;
	time_t current_time;
	int result;
	char *p;

	nInBodyLen = pTask->send.ptr->length - sizeof(FDHTProtoHeader);
	if (nInBodyLen != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != 0", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->send.ptr->length = sizeof(FDHTProtoHeader);
		return EINVAL;
	}

	p = pTask->send.ptr->data + sizeof(FDHTProtoHeader);
	current_time = g_current_time;

	p += sprintf(p, "server=%s:%u\n", g_local_host_ip_addrs+IP_ADDRESS_SIZE
			 , SF_G_OUTER_PORT);
	p += sprintf(p, "version=%d.%02d\n", g_fdht_version.major, g_fdht_version.minor);
	p += sprintf(p, "uptime=%d\n", (int)(current_time-g_server_start_time));
	p += sprintf(p, "curr_time=%d\n", (int)current_time);
	p += sprintf(p, "max_connections=%d\n", SF_G_MAX_CONNECTIONS);
	p += sprintf(p, "curr_connections=%d\n", SF_G_CONN_CURRENT_COUNT);
	p += sprintf(p, "total_set_count=%"PRId64"\n", \
			g_server_stat.total_set_count);
	p += sprintf(p, "success_set_count=%"PRId64"\n", \
			g_server_stat.success_set_count);
	p += sprintf(p, "total_inc_count=%"PRId64"\n", \
			g_server_stat.total_inc_count);
	p += sprintf(p, "success_inc_count=%"PRId64"\n", \
			g_server_stat.success_inc_count);
	p += sprintf(p, "total_delete_count=%"PRId64"\n", \
			g_server_stat.total_delete_count);
	p += sprintf(p, "success_delete_count=%"PRId64"\n", \
			g_server_stat.success_delete_count);
	p += sprintf(p, "total_get_count=%"PRId64"\n", \
			g_server_stat.total_get_count);
	p += sprintf(p, "success_get_count=%"PRId64"\n", \
			g_server_stat.success_get_count);

	if (g_store_type == FDHT_STORE_TYPE_MPOOL)
	{
		#define STAT_MAX_NUM  64
		HashStat hs;
		int stats[STAT_MAX_NUM];

		if ((result=fc_hash_stat(g_hash_array, &hs, stats, \
					STAT_MAX_NUM)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, call fc_hash_stat fail, " \
				"errno: %d, error info: %s", __LINE__, \
				pTask->client_ip, result, STRERROR(result));
			pTask->send.ptr->length = sizeof(FDHTProtoHeader);
			return result;
		}

		p += sprintf(p, "total_items=%d\n", hs.item_count);
		p += sprintf(p, "bucket count=%d\n", hs.capacity);
		p += sprintf(p, "used_bytes=%"PRId64" (%.2f%%)\n",\
			g_hash_array->bytes_used, \
			(100.00 * g_hash_array->bytes_used) / \
			g_hash_array->max_bytes);
		p += sprintf(p, "max bytes=%"PRId64" (100.00%%)\n",\
			g_hash_array->max_bytes);
		p += sprintf(p, "free bytes=%"PRId64" (%.2f%%)\n", \
			g_hash_array->max_bytes - g_hash_array->bytes_used, \
			(100.00 * (g_hash_array->max_bytes - \
			g_hash_array->bytes_used)) / g_hash_array->max_bytes);
		p += sprintf(p, "bucket_used=%d\n", hs.bucket_used);
		p += sprintf(p, "bucket_max_length=%d\n", hs.bucket_max_length);
		p += sprintf(p, "bucket_avg_length=%.4f\n", \
				hs.bucket_avg_length);
	}

	pTask->send.ptr->length = p - pTask->send.ptr->data;
	return 0;
}

