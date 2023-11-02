/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
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
#include "fdht_global.h"
#include "sockopt.h"
#include "logger.h"
#include "shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_func.h"
#include "fdht_client.h"

void usage(const char *program)
{
	printf("Usage: %s <config_file> [namespace:][object id] " \
		"<key list split by comma>\n" \
		"\tsuch as: %s /etc/fdht/fdht_client.conf " \
		"bbs:happyfish name,sex,mail\n", \
		program, program);
}

int main(int argc, char *argv[])
{
	char *conf_filename;
	char *keys_arg;
	char *keys_buff;
	char *parts2[2];
	char **keys;
	FDHTKeyValuePair *key_list;
	FDHTKeyValuePair *pKeyValuePair;
	char object_buff[FDHT_MAX_NAMESPACE_LEN + FDHT_MAX_OBJECT_ID_LEN + 2];
	int result;
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
"Please visit the FastDHT Home Page http://www.csource.org/ \n" \
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
		keys_arg = argv[2];
	}
	else
	{
		keys_arg = argv[3];
		snprintf(object_buff, sizeof(object_buff), "%s", argv[2]);
		if (parseAddress(object_buff, parts2) !=2 )
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

	if (*keys_arg == '\0')
	{
		printf("invalid empty keys!\n");
		return EINVAL;
	}

	log_init();

	g_log_context.log_level = LOG_DEBUG;
	if ((result=fdht_client_init(conf_filename)) != 0)
	{
		return result;
	}

	log_set_cache(false);

	keys_buff = strdup(keys_arg);
	if (keys_buff == NULL)
	{
		printf("strdup %d bytes fail!\n", (int)strlen(keys_arg));
		return ENOMEM;
	}
	
	memset(&key_info, 0, sizeof(key_info));
	keys = split(keys_buff, ',', 0, &key_count);

	key_list = (FDHTKeyValuePair *)malloc(sizeof(FDHTKeyValuePair) \
				* key_count);
	if (key_list == NULL)
	{
		printf("malloc %d bytes fail!\n", \
			(int)sizeof(FDHTKeyValuePair) * key_count);
		return ENOMEM;
	}
	memset(key_list, 0, sizeof(FDHTKeyValuePair) * key_count);

	success_count = 0;
	fail_count = 0;
	pKeyValuePair = key_list;
	for (i=0; i<key_count; i++)
	{
		pKeyValuePair->key_len = snprintf(pKeyValuePair->szKey, \
				sizeof(pKeyValuePair->szKey), "%s", keys[i]);
		if (object_info.namespace_len == 0)
		{
			key_info.key_len = snprintf(key_info.szKey, \
				sizeof(key_info.szKey), "%s", keys[i]);
			if ((pKeyValuePair->status=fdht_delete(&key_info)) != 0)
			{
				fail_count++;
			}
			else
			{
				success_count++;
			}
		}

		pKeyValuePair++;
	}

	if (object_info.namespace_len > 0)
	{
		result = fdht_batch_delete(&object_info, key_list, \
				pKeyValuePair - key_list, &success_count);
		if (result != 0)
		{
			fail_count = pKeyValuePair - key_list;
			printf("delete keys fail, " \
				"error code: %d, error info: %s\n", \
				result, STRERROR(result));
			fdht_client_destroy();
			return result;
		}
		else
		{
			fail_count = (pKeyValuePair - key_list) - success_count;
		}
	}

	if (fail_count > 0)
	{
		pKeyValuePair = key_list;
		for (i=0; i<key_count; i++)
		{
			if (pKeyValuePair->status == 0)
			{
				pKeyValuePair++;
				continue;
			}

			printf("delete key: %s fail, " \
				"error code: %d, error info: %s\n", \
				pKeyValuePair->szKey, pKeyValuePair->status, \
				STRERROR(pKeyValuePair->status));
			pKeyValuePair++;
		}
		printf("\n");
	}

	if (success_count > 0)
	{
		printf("success delete keys: ");
		count = 0;
		pKeyValuePair = key_list;
		for (i=0; i<key_count; i++)
		{
			if (pKeyValuePair->status != 0)
			{
				pKeyValuePair++;
				continue;
			}

			if (count > 0)
			{
				printf(", ");
			}

			printf("%s", pKeyValuePair->szKey);
			pKeyValuePair++;
			count++;
		}
		printf("\n");
	}

	printf("\n");
	printf("success delete key count: %d, fail count: %d\n", \
		success_count, fail_count);

	free(keys_buff);
	free(key_list);
	freeSplit(keys);

	fdht_client_destroy();
	return result;
}

