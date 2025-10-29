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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include "fastcommon/sockopt.h"
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_global.h"
#include "fdht_func.h"
#include "fdht_client.h"

void usage(const char *program)
{
	printf("Usage: %s <config_file> [namespace:][object id] " \
		"<key value pairs, split by comma>\n" \
		"\tsuch as: %s /etc/fdht/fdht_client.conf " \
		"bbs:happyfish name=yq,sex=M,mail=xxx@domain.com\n", \
		program, program);
}

int main(int argc, char *argv[])
{
	char *conf_filename;
	char *kv_arg;
	char *kv_buff;
	char *parts2[2];
	char **kv_pairs;
	char **key_value;
	FDHTKeyValuePair *key_list;
	FDHTKeyValuePair *pKeyValuePair;
	char object_buff[FDHT_MAX_NAMESPACE_LEN + FDHT_MAX_OBJECT_ID_LEN + 2];
	int result;
	int expires;
	int kv_len;
	int key_count;
	int success_count;
	int fail_count;
	int count;
	int i;
	FDHTObjectInfo object_info;
	FDHTKeyInfo key_info;

	printf("This is FastDHT client test program v%d.%02d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDHT may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDHT source kit.\n" \
"Please visit the FastDHT Home Page http://www.fastken.com/ \n" \
"for more detail.\n\n" \
, g_fdht_version.major, g_fdht_version.minor);

	if (argc < 3)
	{
		usage(argv[0]);
		return EINVAL;
	}

	conf_filename = argv[1];
	memset(&object_info, 0, sizeof(FDHTObjectInfo));
	if (argc == 3)
	{
		kv_arg = argv[2];
	}
	else
	{
		kv_arg = argv[3];
		snprintf(object_buff, sizeof(object_buff), "%s", argv[2]);
		if (splitEx(object_buff, ':', parts2, 2) != 2)
		{
			usage(argv[0]);
			return EINVAL;
		}

		object_info.namespace_len = strlen(parts2[0]);
		object_info.obj_id_len = strlen(parts2[1]);
		if (object_info.namespace_len == 0)
		{
			printf("invalid empty namespace!\n");
			return EINVAL;
		}
		if (object_info.namespace_len > FDHT_MAX_NAMESPACE_LEN)
		{
			printf("namespace: %s is too long, exceed %d!\n", \
				parts2[0], FDHT_MAX_NAMESPACE_LEN);
			return EINVAL;
		}

		if (object_info.obj_id_len == 0)
		{
			printf("invalid empty object Id!\n");
			return EINVAL;
		}
		if (object_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN)
		{
			printf("object id: %s is too long, exceed %d!\n", \
				parts2[1], FDHT_MAX_OBJECT_ID_LEN);
			return EINVAL;
		}

		memcpy(object_info.szNameSpace, parts2[0], \
			object_info.namespace_len);
		memcpy(object_info.szObjectId, parts2[1], \
			object_info.obj_id_len);
	}

	kv_len = strlen(kv_arg);
	if (kv_len == 0)
	{
		printf("invalid empty key value pair!\n");
		return EINVAL;
	}

	kv_buff = strdup(kv_arg);
	if (kv_buff == NULL)
	{
		printf("strdup %d bytes fail!\n", (int)strlen(kv_arg));
		return ENOMEM;
	}
	
	log_init();

	g_log_context.log_level = LOG_DEBUG;
	if ((result=fdht_client_init(conf_filename)) != 0)
	{
		return result;
	}

	log_set_cache(false);

	expires = FDHT_EXPIRES_NEVER;
	memset(&key_info, 0, sizeof(key_info));
	kv_pairs = split(kv_buff, ',', 0, &key_count);

	if (object_info.namespace_len > 0)
	{
		key_list = (FDHTKeyValuePair *)malloc(sizeof(FDHTKeyValuePair) \
				* key_count);
		if (key_list == NULL)
		{
			printf("malloc %d bytes fail!\n", \
				(int)sizeof(FDHTKeyValuePair) * key_count);
			return ENOMEM;
		}
		memset(key_list, 0, sizeof(FDHTKeyValuePair) * key_count);
	}
	else
	{
		key_list = NULL;
	}

	success_count = 0;
	fail_count = 0;
	pKeyValuePair = key_list;
	for (i=0; i<key_count; i++)
	{
		key_value = split(kv_pairs[i], '=', 0, &count);
		if (count != 2)
		{
			freeSplit(key_value);
			fail_count++;
			printf("invalid key value pair: %s\n", kv_pairs[i]);
			continue;
		}

		if (object_info.namespace_len > 0)
		{
			pKeyValuePair->key_len = snprintf(pKeyValuePair->szKey, \
				sizeof(pKeyValuePair->szKey), "%s", key_value[0]);
			pKeyValuePair->value_len = strlen(key_value[1]);
			pKeyValuePair->pValue = key_value[1];
			pKeyValuePair++;
		}
		else
		{
			key_info.key_len = snprintf(key_info.szKey, \
				sizeof(key_info.szKey), "%s", key_value[0]);
			if ((result=fdht_set(&key_info, expires, key_value[1], \
					strlen(key_value[1]))) != 0)
			{
				printf("set key value pair: %s=%s fail, " \
					"error code: %d\n", key_value[0], \
					key_value[1], result);
				fail_count++;
			}
			else
			{
				success_count++;
			}
		}

		freeSplit(key_value);
	}

	if (object_info.namespace_len > 0)
	{
		result = fdht_batch_set(&object_info, key_list, \
			pKeyValuePair - key_list, expires, &success_count);
		if (result != 0)
		{
			fail_count = pKeyValuePair - key_list;
			printf("set keys fail, error code: %d\n", result);
		}
		else
		{
			fail_count = (pKeyValuePair - key_list) - success_count;
		}
	}

	free(kv_buff);
	if (key_list != NULL)
	{
		free(key_list);
	}
	freeSplit(kv_pairs);

	printf("success set key count: %d, fail count: %d\n", \
		success_count, fail_count);

	fdht_client_destroy();
	return result;
}

