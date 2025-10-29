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
#include <sys/types.h>
#include "fastcommon/sockopt.h"
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_global.h"
#include "fdht_client.h"
#include "fdht_func.h"

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	int expires;
	FDHTKeyInfo key_info;
	FDHTObjectInfo object_info;
	char szValue[256];
	char sub_keys[16 * 1024];
	int value_len;
	int i;
	char stat_buff[1024];

	printf("This is FastDHT client test program v%d.%02d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDHT may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDHT source kit.\n" \
"Please visit the FastDHT Home Page http://www.csource.org/ \n" \
"for more detail.\n\n" \
, g_fdht_version.major, g_fdht_version.minor);

	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	log_init();
	conf_filename = argv[1];

	g_log_context.log_level = LOG_DEBUG;
	if ((result=fdht_client_init(conf_filename)) != 0)
	{
		return result;
	}

	srand(time(NULL));

	//expires = time(NULL) + 3600;
	expires = FDHT_EXPIRES_NEVER;
	memset(&key_info, 0, sizeof(key_info));
	key_info.namespace_len = sprintf(key_info.szNameSpace, "user");
	key_info.obj_id_len = sprintf(key_info.szObjectId, "happy_fish");
	key_info.key_len = sprintf(key_info.szKey, "reg");

	memset(&object_info, 0, sizeof(object_info));
	object_info.namespace_len = key_info.namespace_len;
	object_info.obj_id_len = key_info.obj_id_len;

	memcpy(object_info.szNameSpace, key_info.szNameSpace, \
		key_info.namespace_len);
	memcpy(object_info.szObjectId, key_info.szObjectId, \
		key_info.obj_id_len);

	if ((result=fdht_get_sub_keys(&object_info, sub_keys, \
		sizeof(sub_keys))) != 0)
	{
		printf("fdht_get_sub_keys fail, errno: %d, error info: %s\n", \
			result, STRERROR(result));
	}
	else
	{
		printf("sub keys: %s\n", sub_keys);
	}

	//key_info.obj_id_len = sprintf(key_info.szObjectId, "o%d", 1234567);
	//key_info.key_len = sprintf(key_info.szKey, "k%d", 97865432);

	while (1)
	{
		char *value;

		//memset(szValue, '1', sizeof(szValue));
		value_len = sprintf(szValue, "%d", rand());

		printf("original value=%s(%d)\n", szValue, value_len);

		if ((result=fdht_set(&key_info, expires, szValue, value_len)) != 0)
		{
			break;
		}

		value_len = sizeof(szValue);
		if ((result=fdht_inc(&key_info, expires, 100, \
				szValue, &value_len)) != 0)
		{
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", szValue);

		value = szValue;
		value_len = sizeof(szValue);
		if ((result=fdht_get_ex(&key_info, expires, &value, &value_len)) != 0)
		{
			printf("fdht_get_ex result=%d\n", result);
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", value);
		/*
		if ((result=fdht_delete(&key_info)) != 0)
		{
			break;
		}
		*/
		break;
	}

	printf("\n");
	for (i=0; i<g_group_array.server_count; i++)
	{
		if ((result=fdht_stat(i, stat_buff, sizeof(stat_buff))) != 0)
		{
			printf("fdht_stat server %s:%u fail, errno: %d\n", 
				g_group_array.servers[i].ip_addr, 
				g_group_array.servers[i].port, result);
		}
		else
		{
			printf("server %s:%u\n", g_group_array.servers[i].ip_addr, \
				g_group_array.servers[i].port);
			printf("%s\n", stat_buff);
		}
	}

	if ((result=fdht_get_sub_keys(&object_info, sub_keys, \
		sizeof(sub_keys))) != 0)
	{
		printf("fdht_get_sub_keys fail, errno: %d, error info: %s\n", \
			result, STRERROR(result));
	}
	else
	{
		printf("sub keys: %s\n", sub_keys);
	}

	if (g_keep_alive)
	{
		fdht_disconnect_all_servers(&g_group_array);
	}
	
	fdht_client_destroy();

	return result;
}

