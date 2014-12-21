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
#include "fdht_global.h"
#include "sockopt.h"
#include "logger.h"
#include "shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_client.h"
#include "fdht_func.h"

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	int expires;
	FDHTKeyInfo key_info;
	int conn_success_count;
	int conn_fail_count;
	char szValue[256];
	int value_len;
	GroupArray group_array;
	bool bKeepAlive;

	printf("This is FastDHT client test program v%d.%d\n" \
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

	if ((result=fdht_copy_group_array(&group_array, &g_group_array)) != 0)
	{
		return result;
	}

	bKeepAlive = true;
	if (bKeepAlive)
	{
		if ((result=fdht_connect_all_servers(&group_array, true, \
			&conn_success_count, &conn_fail_count)) != 0)
		{
			printf("fdht_connect_all_servers fail, " \
				"error code: %d, error info: %s\n", \
				result, STRERROR(result));
		}
	}

	srand(time(NULL));

	expires = time(NULL) + 3600;
	memset(&key_info, 0, sizeof(key_info));
	key_info.namespace_len = sprintf(key_info.szNameSpace, "bbs");
	key_info.obj_id_len = sprintf(key_info.szObjectId, "o%d", rand());
	key_info.key_len = sprintf(key_info.szKey, "k%d", rand());

	/*
	key_info.obj_id_len = sprintf(key_info.szObjectId, "o%d", 12345678);
	key_info.key_len = sprintf(key_info.szKey, "k%d", 978654);
	*/

	while (1)
	{
		char *value;

		//memset(szValue, '1', sizeof(szValue));
		value_len = sprintf(szValue, "%d", rand());

		printf("original value=%s(%d)\n", szValue, value_len);

		if ((result=fdht_set_ex(&group_array, bKeepAlive, &key_info, \
				expires, szValue, value_len)) != 0)
		{
			break;
		}

		value_len = sizeof(szValue);
		if ((result=fdht_inc_ex(&group_array, bKeepAlive, &key_info, \
				expires, 100, szValue, &value_len)) != 0)
		{
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", szValue);

		value = szValue;
		value_len = sizeof(szValue);
		if ((result=fdht_get_ex1(&group_array, bKeepAlive, &key_info, \
				expires, &value, &value_len, malloc)) != 0)
		{
			printf("result=%d\n", result);
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", value);
		if ((result=fdht_delete_ex(&group_array, bKeepAlive, \
				&key_info)) != 0)
		{
			break;
		}

		break;
	}

	if (bKeepAlive)
	{
		fdht_disconnect_all_servers(&group_array);
	}

	fdht_free_group_array(&group_array);
	
	fdht_client_destroy();

	return result;
}

