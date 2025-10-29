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

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	int success_count;
	int fail_count;
	int expires;
	FDHTKeyInfo key_info;
	char szValue[101];
	char *pValue;
	int value_len;
	struct sigaction act;
	int i;

	printf("This is FastDHT client test program v%d.%02d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDHT may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDHT source kit.\n" \
"Please visit the FastDHT Home Page http://www.fastken.com/ \n" \
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

	log_set_prefix(g_fdht_base_path, "fdht_test_get");
	log_set_cache(false);

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		return errno;
	}

	srand(time(NULL));

	expires = 0;
	memset(&key_info, 0, sizeof(key_info));

	value_len = sizeof(szValue) - 1;
	for (i=0; i<value_len; i++)
	{
		szValue[i] = (char)rand();
	}

	g_keep_alive = true;
	if (g_keep_alive)
	{
		if ((result=fdht_connect_all_servers(&g_group_array, true, \
			&success_count, &fail_count)) != 0)
		{
			printf("fdht_connect_all_servers fail, " \
				"error code: %d, error info: %s\n", \
				result, STRERROR(result));
			return result;
		}
	}

	key_info.key_len = sprintf(key_info.szKey, "k%015d", rand());
	if ((result=fdht_set(&key_info, expires, szValue, value_len)) != 0)
	{
		return result;
	}

	pValue = szValue;
	success_count = 0;
	fail_count = 0;
	for (i=1; i<=100000; i++)
	{
		if (i % 10000 == 0)
		{
			printf("current: %d\n", i);
			fflush(stdout);
		}

		value_len = sizeof(szValue);
		if ((result=fdht_get(&key_info, &pValue, &value_len)) != 0)
		{
			logError("fdht_get return %d", result);
			fail_count++;
		}
		else
		{
			success_count++;
		}
	}

	if (g_keep_alive)
	{
		fdht_disconnect_all_servers(&g_group_array);
	}

	fdht_client_destroy();

	printf("success count=%d, fail count=%d\n", success_count, fail_count);

	return result;
}

