//func.c

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
#include "pthread_func.h"
#include "sched_thread.h"
#include "ini_file_reader.h"
#include "fdht_global.h"
#include "global.h"
#include "fdht_func.h"
#include "fast_task_queue.h"
#include "sync.h"
#include "func.h"
#include "store.h"
#include "db_op.h"
#include "mpool_op.h"
#include "key_op.h"

#define DB_FILE_PREFIX_MAX_SIZE  32
#define FDHT_STAT_FILENAME		"stat.dat"
#define STAT_ITEM_TOTAL_SET		"total_set_count"
#define STAT_ITEM_SUCCESS_SET		"success_set_count"
#define STAT_ITEM_TOTAL_GET		"total_get_count"
#define STAT_ITEM_SUCCESS_GET		"success_get_count"
#define STAT_ITEM_TOTAL_INC		"total_inc_count"
#define STAT_ITEM_SUCCESS_INC		"success_inc_count"
#define STAT_ITEM_TOTAL_DELETE		"total_delete_count"
#define STAT_ITEM_SUCCESS_DELETE	"success_delete_count"

StoreHandle **g_db_list = NULL;
int g_db_count = 0;

static pthread_t dld_tid = 0;

static int fdht_stat_fd = -1;
static FDHTServerStat fdht_last_stat;

int group_cmp_by_ip_and_port(const void *p1, const void *p2)
{
	int res;

	res = strcmp(((FDHTGroupServer *)p1)->ip_addr, \
			((FDHTGroupServer *)p2)->ip_addr);
	if (res != 0)
	{
		return res;
	}

	return ((FDHTGroupServer *)p1)->port - \
			((FDHTGroupServer *)p2)->port;
}

static int load_group_servers(GroupArray *pGroupArray, \
		int *group_ids, const int group_count, \
		FDHTGroupServer **ppGroupServers, int *server_count)
{
	ServerArray *pServerArray;
	FDHTServerInfo **ppServerInfo;
	FDHTServerInfo **ppServerEnd;
	FDHTGroupServer *pFound;
	FDHTGroupServer targetServer;
	int *counts;
	int group_servers;
	int compare;
	int result;
	int id;
	int k;
	int i;

	*ppGroupServers = NULL;
	*server_count = 0;

	id = group_ids[0];
	pServerArray = pGroupArray->groups + id;
	*ppGroupServers = (FDHTGroupServer*)malloc( \
				sizeof(FDHTGroupServer) * pServerArray->count);
	if (*ppGroupServers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, (int)sizeof(FDHTGroupServer) * \
			pServerArray->count, errno, STRERROR(errno));

		return errno != 0 ? errno : ENOMEM;
	}

	memset(*ppGroupServers, 0, sizeof(FDHTGroupServer)*pServerArray->count);
	memset(&targetServer, 0, sizeof(FDHTGroupServer));

	ppServerEnd = pServerArray->servers + pServerArray->count;
	for (ppServerInfo=pServerArray->servers; \
		ppServerInfo<ppServerEnd; ppServerInfo++)
	{
		strcpy(targetServer.ip_addr, (*ppServerInfo)->ip_addr);
		targetServer.port = (*ppServerInfo)->port;
		compare = 1;
		for (k=0; k<*server_count; k++)
		{
			compare = group_cmp_by_ip_and_port(&targetServer, \
						(*ppGroupServers) + k);
			if (compare <= 0)
			{
				break;
			}
		}

		if (compare == 0)
		{
			continue;
		}

		for (i=*server_count-1; i>=k; i--)
		{
			memcpy((*ppGroupServers) + (i+1), \
				(*ppGroupServers) + i, sizeof(FDHTGroupServer));
		}
		strcpy((*ppGroupServers)[k].ip_addr, (*ppServerInfo)->ip_addr);
		(*ppGroupServers)[k].port = (*ppServerInfo)->port;

		(*server_count)++;
	}

	counts = (int *)malloc(sizeof(int) * (*server_count));
	if (counts == NULL)
	{
		free(*ppGroupServers);
		*ppGroupServers = NULL;
		*server_count = 0;

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, (int)sizeof(int) * (*server_count), \
			errno, STRERROR(errno));

		return errno != 0 ? errno : ENOMEM;
	}

	result = 0;
	for (k=1; k < group_count && result == 0; k++)
	{
		group_servers = 0;
		memset(counts, 0, sizeof(int) * (*server_count));

		pServerArray = pGroupArray->groups + group_ids[k];
		ppServerEnd = pServerArray->servers + pServerArray->count;
		for (ppServerInfo=pServerArray->servers; \
			ppServerInfo<ppServerEnd; ppServerInfo++)
		{
			strcpy(targetServer.ip_addr, (*ppServerInfo)->ip_addr);
			targetServer.port = (*ppServerInfo)->port;
			pFound = (FDHTGroupServer *)bsearch(&targetServer, \
				*ppGroupServers, *server_count, \
				sizeof(FDHTGroupServer),group_cmp_by_ip_and_port);
			if (pFound == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"group %d and group %d: " \
					"servers not same, group %d " \
					"no server \"%s:%d\"", __LINE__, \
					group_ids[0], group_ids[k], \
					group_ids[0], (*ppServerInfo)->ip_addr,\
					(*ppServerInfo)->port);
				result = EINVAL;
				break;
			}
			else
			{
				if (counts[pFound - (*ppGroupServers)]++ == 0)
				{
					group_servers++;
				}
			}
		}

		if (group_servers != *server_count)
		{
			logError("file: "__FILE__", line: %d, " \
				"group %d server count: %d, " \
				"group %d server count: %d, " \
				"servers not same", __LINE__, \
				group_ids[0], *server_count, \
				group_ids[k], group_servers);
			result = EINVAL;
			break;
		}
	}

	free(counts);
	if (result != 0)
	{
		free(*ppGroupServers);
		*ppGroupServers = NULL;
		*server_count = 0;
	}

	return result;
}

static int load_group_ids(GroupArray *pGroupArray, \
		const char *bind_addr, int **group_ids, int *group_count)
{
#define MAX_HOST_ADDRS	10
#define FDHT_MAX_ALIAS_PREFIX_COUNT  4

	int result;
	char host_addrs[MAX_HOST_ADDRS][IP_ADDRESS_SIZE];
	char szIfAliasPrefix[FDHT_IF_ALIAS_PREFIX_MAX_SIZE];
	int addrs_count;
	int alias_count;
	char *if_alias_prefixes[FDHT_MAX_ALIAS_PREFIX_COUNT];
	ServerArray *pServerArray;
	ServerArray *pArrayEnd;
	FDHTServerInfo **ppServerInfo;
	FDHTServerInfo **ppServerEnd;
	int id;
	int k;

	*group_ids = NULL;
	*group_count = 0;

	if (*bind_addr != '\0')
	{
		addrs_count = 1;
		snprintf(host_addrs[0], IP_ADDRESS_SIZE, "%s", bind_addr);
	}
	else
	{
		strcpy(szIfAliasPrefix, g_if_alias_prefix);
		memset(if_alias_prefixes, 0, sizeof(if_alias_prefixes));
		if (*szIfAliasPrefix == '\0')
		{
			alias_count = 0;
		}
		else
		{
			alias_count = splitEx(szIfAliasPrefix, ',', \
				if_alias_prefixes, FDHT_MAX_ALIAS_PREFIX_COUNT);
			for (k=0; k<alias_count; k++)
			{
				trim(if_alias_prefixes[k]);
			}
		}

		if ((result=gethostaddrs(if_alias_prefixes, alias_count, \
			host_addrs, MAX_HOST_ADDRS, &addrs_count)) != 0)
		{
			return result;
		}

		if (addrs_count == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"can't get ip address from local host", \
				__LINE__);
			return ENOENT;
		}

		/*
		for (k=0; k < addrs_count; k++)
		{
			printf("%d. ip addr: %s\n", k+1, host_addrs[k]);
		}
		*/
	}

	*group_ids = (int *)malloc(sizeof(int) * pGroupArray->group_count);
	if (*group_ids == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, (int)sizeof(int) * pGroupArray->group_count, \
			errno, STRERROR(errno));

		return errno != 0 ? errno : ENOMEM;
	}

	id = 0;
	pArrayEnd = pGroupArray->groups + pGroupArray->group_count;
	for (pServerArray=pGroupArray->groups; pServerArray<pArrayEnd;
		 pServerArray++)
	{
		if (pServerArray->servers == NULL)
		{
			id++;
			continue;
		}

		ppServerEnd = pServerArray->servers+pServerArray->count;
		for (ppServerInfo=pServerArray->servers; \
			ppServerInfo<ppServerEnd; ppServerInfo++)
		{
			for (k=0; k < addrs_count; k++)
			{
				if (strcmp(host_addrs[k], \
					(*ppServerInfo)->ip_addr) == 0)
				{
					(*group_ids)[*group_count] = id;
					(*group_count)++;
					break;
				}
			}

			if (k < addrs_count)  //found
			{
				break;
			}
		}

		id++;
	}

	if (*group_count == 0)
	{
		free(*group_ids);
		*group_ids = NULL;

		logError("file: "__FILE__", line: %d, " \
			"local host does not belong to any group, " \
			"program exit!", __LINE__);
		return ENOENT;
	}

	return 0;
}

static char *fdht_get_stat_filename(const void *pArg, char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/%s", g_fdht_base_path, \
			FDHT_STAT_FILENAME);
	return full_filename;
}

static int fdht_load_from_conf_file(const char *filename, char *bind_addr, \
		const int addr_size, int **group_ids, int *group_count, \
		DBType *db_type, int64_t *nCacheSize, int *page_size, \
		char *db_file_prefix)
{
	char *pBasePath;
	char *pBindAddr;
	char *pDbType;
	char *pDbFilePrefix;
	char *pRunByGroup;
	char *pRunByUser;
	char *pCacheSize;
	char *pPageSize;
	char *pMaxPkgSize;
	char *pMinBuffSize;
	char *pStoreType;
	char *pThreadStackSize;
	char *pIfAliasPrefix;
	IniContext iniContext;
	int result;
	int64_t nPageSize;
	int64_t max_pkg_size;
	int64_t min_buff_size;
	int64_t thread_stack_size;
	GroupArray groupArray;
	char sz_sync_db_time_base[16];
	char sz_clear_expired_time_base[16];
	char sz_compress_binlog_time_base[16];
	char szStoreParams[256];

	if ((result=iniLoadFromFile(filename, &iniContext)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	//iniPrintItems(&iniContext);
	do
	{
		if (iniGetBoolValue(NULL, "disabled", &iniContext, false))
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" disabled=true, exit", \
				__LINE__, filename);
			result = ECANCELED;
			break;
		}

		pBasePath = iniGetStrValue(NULL, "base_path", &iniContext);
		if (pBasePath == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" must have item " \
				"\"base_path\"!", \
				__LINE__, filename);
			result = ENOENT;
			break;
		}

		snprintf(g_fdht_base_path, sizeof(g_fdht_base_path), "%s", pBasePath);
		chopPath(g_fdht_base_path);
		if (!fileExists(g_fdht_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, STRERROR(errno), g_fdht_base_path);
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_fdht_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" is not a directory!", \
				__LINE__, g_fdht_base_path);
			result = ENOTDIR;
			break;
		}

		load_log_level(&iniContext);
		if ((result=log_set_prefix(g_fdht_base_path, "fdhtd")) != 0)
		{
			break;
		}

		g_fdht_connect_timeout = iniGetIntValue(NULL, "connect_timeout", \
				&iniContext, DEFAULT_CONNECT_TIMEOUT);
		if (g_fdht_connect_timeout <= 0)
		{
			g_fdht_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
		}

		g_fdht_network_timeout = iniGetIntValue(NULL, "network_timeout", \
				&iniContext, DEFAULT_NETWORK_TIMEOUT);
		if (g_fdht_network_timeout <= 0)
		{
			g_fdht_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		g_network_tv.tv_sec = g_fdht_network_timeout;
		g_heart_beat_interval = g_fdht_network_timeout / 2;
		if (g_heart_beat_interval <= 0)
		{
			g_heart_beat_interval = 1;
		}

		g_server_port = iniGetIntValue(NULL, "port", &iniContext, \
					FDHT_SERVER_DEFAULT_PORT);
		if (g_server_port <= 0)
		{
			g_server_port = FDHT_SERVER_DEFAULT_PORT;
		}

		pBindAddr = iniGetStrValue(NULL, "bind_addr", &iniContext);
		if (pBindAddr == NULL)
		{
			bind_addr[0] = '\0';
		}
		else
		{
			snprintf(bind_addr, addr_size, "%s", pBindAddr);
		}

		g_max_connections = iniGetIntValue(NULL, "max_connections", \
				&iniContext, DEFAULT_MAX_CONNECTONS);
		if (g_max_connections <= 0)
		{
			g_max_connections = DEFAULT_MAX_CONNECTONS;
		}
		if ((result=set_rlimit(RLIMIT_NOFILE, g_max_connections)) != 0)
		{
			break;
		}

		pStoreType = iniGetStrValue(NULL, "store_type", &iniContext);
		if (pStoreType == NULL)
		{
			g_store_type = FDHT_STORE_TYPE_BDB;
		}
		else if (strcasecmp(pStoreType, "BDB") == 0)
		{
			g_store_type = FDHT_STORE_TYPE_BDB;
		}
		else if (strcasecmp(pStoreType, "MPOOL") == 0)
		{
			g_store_type = FDHT_STORE_TYPE_MPOOL;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"item \"store_type\" is invalid, " \
				"value: \"%s\"", __LINE__, pStoreType);
			result = EINVAL;
			break;
		}

		if (g_store_type == FDHT_STORE_TYPE_MPOOL)
		{
			g_mpool_init_capacity = iniGetIntValue(NULL,  \
				"mpool_init_capacity", &iniContext, \
				FDHT_DEFAULT_MPOOL_INIT_CAPACITY);
			if (g_mpool_init_capacity < 0)
			{
				g_mpool_init_capacity  = \
					FDHT_DEFAULT_MPOOL_INIT_CAPACITY;
			}

			g_mpool_load_factor = iniGetDoubleValue(NULL, \
				"mpool_load_factor", &iniContext, \
				FDHT_DEFAULT_MPOOL_LOAD_FACTOR);

			g_mpool_clear_min_interval = iniGetIntValue(NULL,  \
				"mpool_clear_min_interval", &iniContext, \
				FDHT_DEFAULT_MPOOL_CLEAR_MIN_INTEVAL);
			if (g_mpool_clear_min_interval <= 0)
			{
				g_mpool_clear_min_interval = \
					FDHT_DEFAULT_MPOOL_CLEAR_MIN_INTEVAL;
			}

			g_mpool_htable_lock_count = iniGetIntValue(NULL,  \
				"mpool_htable_lock_count", &iniContext, \
				FDHT_DEFAULT_MPOOL_HTABLE_LOCK_COUNT);
			if (g_mpool_htable_lock_count <= 0)
			{
				g_mpool_htable_lock_count = \
					FDHT_DEFAULT_MPOOL_HTABLE_LOCK_COUNT;
			}

			snprintf(szStoreParams, sizeof(szStoreParams), \
				"mpool_init_capacity=%d, " \
				"mpool_load_factor=%.2f, " \
				"mpool_clear_min_interval=%ds, " \
				"mpool_htable_lock_count=%d", \
				g_mpool_init_capacity, g_mpool_load_factor, \
				g_mpool_clear_min_interval,
				g_mpool_htable_lock_count);
		}
		else
		{
			pDbType = iniGetStrValue(NULL, "db_type", &iniContext);
			if (pDbType == NULL)
			{
				*db_type = DB_BTREE;
			}
			else if (strcasecmp(pDbType, "btree") == 0)
			{
				*db_type = DB_BTREE;
			}
			else if (strcasecmp(pDbType, "hash") == 0) 
			{
				*db_type = DB_HASH;
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"item \"db_type\" is invalid, " \
					"value: \"%s\"", __LINE__, pDbType);
				result = EINVAL;
				break;
			}

			nPageSize = 4 * 1024;
			pPageSize = iniGetStrValue(NULL, "page_size", &iniContext);
			if (pPageSize != NULL && (result=parse_bytes( \
				pPageSize, 1, &nPageSize)) != 0)
			{
				break;
			}

			if ((nPageSize < 512) || (nPageSize > 64 * 1024))
			{
				logError("file: "__FILE__", line: %d, " \
					"page_size: %"PRId64 \
					"is invalid, which < %d or > %d!", \
					__LINE__, nPageSize, 512, 64 * 1024);
				result = EINVAL;
				break;
			}
			*page_size = (int)nPageSize;

			pDbFilePrefix = iniGetStrValue(NULL, "db_prefix", &iniContext);
			if (pDbFilePrefix == NULL || *pDbFilePrefix == '\0')
			{
				logError("file: "__FILE__", line: %d, " \
					"item \"db_prefix\" not exist or " \
					"is empty!", __LINE__);
				result = ENOENT;
				break;
			}
			snprintf(db_file_prefix, DB_FILE_PREFIX_MAX_SIZE, \
				"%s", pDbFilePrefix);
			g_sync_db_interval = iniGetIntValue(NULL,  \
				"sync_db_interval", &iniContext, \
				DEFAULT_SYNC_DB_INVERVAL);

			if ((result=get_time_item_from_conf(&iniContext, \
				"sync_db_time_base", &g_sync_db_time_base, \
				0, 0)) != 0)
			{
				break;
			}

			if (g_sync_db_time_base.hour == TIME_NONE)
			{
				strcpy(sz_sync_db_time_base, "current time");
			}
			else
			{
				sprintf(sz_sync_db_time_base, "%02d:%02d", \
						g_sync_db_time_base.hour, \
						g_sync_db_time_base.minute);
			}
			g_db_dead_lock_detect_interval = iniGetIntValue(NULL,  \
				"db_dead_lock_detect_interval", &iniContext, \
				DEFAULT_DB_DEAD_LOCK_DETECT_INVERVAL);

			snprintf(szStoreParams, sizeof(szStoreParams), \
				"db_type=%s, " \
				"db_prefix=%s, " \
				"page_size=%d, " \
				"sync_db_time_base=%s, sync_db_interval=%ds, " \
				"db_dead_lock_detect_interval=%dms", \
				*db_type == DB_BTREE ? "btree" : "hash", \
				db_file_prefix, *page_size, \
				sz_sync_db_time_base, g_sync_db_interval, \
				g_db_dead_lock_detect_interval);
		}

		g_max_threads = iniGetIntValue(NULL, "max_threads", &iniContext, \
					FDHT_DEFAULT_MAX_THREADS);
		if (g_max_threads <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"item \"max_threads\" is invalid, " \
				"value: %d <= 0!", __LINE__, g_max_threads);
			result = EINVAL;
			break;
		}

		g_accept_threads = iniGetIntValue(NULL, "accept_threads",
					&iniContext, 1);
		if (g_accept_threads <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"item \"accept_threads\" is invalid, " \
				"value: %d <= 0!", __LINE__, g_accept_threads);
			result = EINVAL;
			break;
		}

		pMaxPkgSize = iniGetStrValue(NULL, "max_pkg_size", &iniContext);
		if (pMaxPkgSize == NULL)
		{
			g_max_pkg_size = FDHT_MAX_PKG_SIZE;
		}
		else 
		{
			if ((result=parse_bytes(pMaxPkgSize, 1, \
					&max_pkg_size)) != 0)
			{
				return result;
			}
			g_max_pkg_size = (int)max_pkg_size;
		}

		pMinBuffSize = iniGetStrValue(NULL, "min_buff_size", &iniContext);
		if (pMinBuffSize == NULL)
		{
			g_min_buff_size = FDHT_MIN_BUFF_SIZE;
		}
		else
		{
			if ((result=parse_bytes(pMinBuffSize, 1, \
					&min_buff_size)) != 0)
			{
				return result;
			}
			g_min_buff_size = (int)min_buff_size;
			if (g_min_buff_size < 1024)
			{
				g_min_buff_size = 1024;
			}
		}

		g_sync_wait_usec = iniGetIntValue(NULL, "sync_wait_msec", \
			 &iniContext, DEFAULT_SYNC_WAIT_MSEC);
		if (g_sync_wait_usec <= 0)
		{
			g_sync_wait_usec = DEFAULT_SYNC_WAIT_MSEC;
		}
		g_sync_wait_usec *= 1000;

		
		pRunByGroup = iniGetStrValue(NULL, "run_by_group", &iniContext);
		pRunByUser = iniGetStrValue(NULL, "run_by_user", &iniContext);
		if ((result=set_run_by(pRunByGroup, pRunByUser)) != 0)
		{
			break;
		}

		if ((result=load_allow_hosts(&iniContext, \
                	 &g_allow_ip_addrs, &g_allow_ip_count)) != 0)
		{
			break;
		}

		pCacheSize = iniGetStrValue(NULL, "cache_size", &iniContext);
		if (pCacheSize == NULL)
		{
			*nCacheSize = 64 * 1024 * 1024;
		}
		else if ((result=parse_bytes(pCacheSize, 1, nCacheSize)) != 0)
		{
			break;
		}

		if (*nCacheSize < 1024 * 1024)
		{
			*nCacheSize = 1024 * 1024;
		}

		pIfAliasPrefix = iniGetStrValue(NULL, \
			"if_alias_prefix", &iniContext);
		if (pIfAliasPrefix == NULL)
		{
			*g_if_alias_prefix = '\0';
		}
		else
		{
			snprintf(g_if_alias_prefix, sizeof(g_if_alias_prefix), 
				"%s", pIfAliasPrefix);
		}

		memset(&groupArray, 0, sizeof(groupArray));
		result = fdht_load_groups(&iniContext, &groupArray);
		if (result != 0)
		{
			break;
		}

		g_group_count = groupArray.group_count;
		if ((result=load_group_ids(&groupArray, bind_addr, \
				group_ids, group_count)) != 0)
		{
			fdht_free_group_array(&groupArray);
			break;
		}

		result = load_group_servers(&groupArray, *group_ids, \
			*group_count, &g_group_servers, &g_group_server_count);
		fdht_free_group_array(&groupArray);
		if (result != 0)
		{
			free(*group_ids);
			*group_ids = NULL;
			break;
		}

		/*
		{
		int i;
		for (i=0; i<g_group_server_count; i++)
		{
			//printf("%d. %s:%d\n", i+1, g_group_servers[i].ip_addr, g_group_servers[i].port);
		}
		}
		*/

		g_sync_log_buff_interval = iniGetIntValue(NULL,  \
				"sync_log_buff_interval", &iniContext, \
				SYNC_LOG_BUFF_DEF_INTERVAL);
		if (g_sync_log_buff_interval <= 0)
		{
			g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
		}



		g_need_clear_expired_data = iniGetBoolValue(NULL,  \
				"need_clear_expired_data", &iniContext, true);
		g_clear_expired_interval = iniGetIntValue(NULL,  \
				"clear_expired_interval", &iniContext, \
				DEFAULT_CLEAR_EXPIRED_INVERVAL);
		
		if ((result=get_time_item_from_conf(&iniContext, \
			"clear_expired_time_base", &g_clear_expired_time_base, \
			4, 0)) != 0)
		{
			break;
		}

		if (g_clear_expired_time_base.hour == TIME_NONE)
		{
			strcpy(sz_clear_expired_time_base, "current time");
		}
		else
		{
			sprintf(sz_clear_expired_time_base, "%02d:%02d", \
				g_clear_expired_time_base.hour, \
				g_clear_expired_time_base.minute);
		}

		g_write_to_binlog_flag = iniGetBoolValue(NULL, "write_to_binlog", \
					&iniContext, true);

		g_sync_binlog_buff_interval = iniGetIntValue(NULL,  \
				"sync_binlog_buff_interval", &iniContext, \
				SYNC_BINLOG_BUFF_DEF_INTERVAL);
		if (g_sync_binlog_buff_interval <= 0)
		{
			g_sync_binlog_buff_interval = SYNC_BINLOG_BUFF_DEF_INTERVAL;
		}

		if ((result=get_time_item_from_conf(&iniContext, \
			"compress_binlog_time_base", &g_compress_binlog_time_base, \
			2, 0)) != 0)
		{
			break;
		}

		if (g_compress_binlog_time_base.hour == TIME_NONE)
		{
			strcpy(sz_compress_binlog_time_base, "current time");
		}
		else
		{
			sprintf(sz_compress_binlog_time_base, "%02d:%02d", \
				g_compress_binlog_time_base.hour, \
				g_compress_binlog_time_base.minute);
		}

		g_compress_binlog_interval = iniGetIntValue(NULL,  \
			"compress_binlog_interval", &iniContext, \
			COMPRESS_BINLOG_DEF_INTERVAL);

		g_sync_stat_file_interval = iniGetIntValue(NULL,  \
				"sync_stat_file_interval", &iniContext, \
				DEFAULT_SYNC_STAT_FILE_INTERVAL);
		if (g_sync_stat_file_interval <= 0)
		{
			g_sync_stat_file_interval = DEFAULT_SYNC_STAT_FILE_INTERVAL;
		}

		g_write_mark_file_freq = iniGetIntValue(NULL,  \
				"write_mark_file_freq", &iniContext, \
				FDHT_DEFAULT_SYNC_MARK_FILE_FREQ);
		if (g_write_mark_file_freq <= 0)
		{
			g_write_mark_file_freq = FDHT_DEFAULT_SYNC_MARK_FILE_FREQ;
		}

		pThreadStackSize = iniGetStrValue(NULL,  \
			"thread_stack_size", &iniContext);
		if (pThreadStackSize == NULL)
		{
			thread_stack_size = 1 * 1024 * 1024;
		}
		else if ((result=parse_bytes(pThreadStackSize, 1, \
				&thread_stack_size)) != 0)
		{
			return result;
		}
		g_thread_stack_size = (int)thread_stack_size;

		g_store_sub_keys = iniGetBoolValue(NULL, "store_sub_keys", \
						&iniContext, false);

		logInfo("FastDHT v%d.%02d, base_path=%s, " \
			"total group count=%d, my group count=%d, " \
			"group server count=%d, " \
			"connect_timeout=%d, "\
			"network_timeout=%d, "\
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"accept_threads=%d, "    \
			"max_threads=%d, "    \
			"max_pkg_size=%d KB, " \
			"min_buff_size=%d KB, " \
			"store_type=%s, " \
			"cache_size=%d MB, %s, " \
			"sync_wait_msec=%dms, "  \
			"allow_ip_count=%d, sync_log_buff_interval=%ds, " \
			"need_clear_expired_data=%d, " \
			"clear_expired_time_base=%s, " \
			"clear_expired_interval=%ds, " \
			"write_to_binlog=%d, sync_binlog_buff_interval=%ds, " \
			"compress_binlog_time_base=%s, " \
			"compress_binlog_interval=%ds, " \
			"sync_stat_file_interval=%ds, " \
			"write_mark_file_freq=%d, " \
			"thread_stack_size=%d KB, if_alias_prefix=%s, " \
			"store_sub_keys=%d",  \
			g_fdht_version.major, g_fdht_version.minor, \
			g_fdht_base_path, g_group_count, *group_count, \
			g_group_server_count, g_fdht_connect_timeout, \
			g_fdht_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_accept_threads, g_max_threads, g_max_pkg_size / 1024, \
			g_min_buff_size / 1024, \
			g_store_type == FDHT_STORE_TYPE_BDB ? "BDB" : "MPOOL", \
			(int)(*nCacheSize / (1024 * 1024)), szStoreParams, \
			g_sync_wait_usec / 1000, \
			g_allow_ip_count, g_sync_log_buff_interval, \
			g_need_clear_expired_data, \
			sz_clear_expired_time_base, g_clear_expired_interval, \
			g_write_to_binlog_flag, \
			g_sync_binlog_buff_interval, \
			sz_compress_binlog_time_base, \
			g_compress_binlog_interval, g_sync_stat_file_interval, \
 			g_write_mark_file_freq, g_thread_stack_size/1024, \
			g_if_alias_prefix, g_store_sub_keys);
	} while (0);

	iniFreeContext(&iniContext);

	return result;
}

static int fdht_load_stat_from_file()
{
	char full_filename[MAX_PATH_SIZE];
	char data_path[MAX_PATH_SIZE];
	IniContext iniContext;
	int result;

	memset(&g_server_stat, 0, sizeof(g_server_stat));
	memset(&fdht_last_stat, 0, sizeof(fdht_last_stat));

	snprintf(data_path, sizeof(data_path), "%s/data", g_fdht_base_path);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, errno, STRERROR(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	fdht_get_stat_filename(NULL, full_filename);
	if (fileExists(full_filename))
	{
		if ((result=iniLoadFromFile(full_filename, &iniContext)) \
			 != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from stat file \"%s\" fail, " \
				"error code: %d", \
				__LINE__, full_filename, result);
			return result;
		}

		if (iniContext.global.count < 8)
		{
			iniFreeContext(&iniContext);
			logError("file: "__FILE__", line: %d, " \
				"in stat file \"%s\", item count: %d < 8", \
				__LINE__, full_filename, iniContext.global.count);
			return ENOENT;
		}

		g_server_stat.total_set_count = iniGetInt64Value(NULL, \
				STAT_ITEM_TOTAL_SET, \
				&iniContext, 0);
		g_server_stat.success_set_count = iniGetInt64Value(NULL, \
				STAT_ITEM_SUCCESS_SET, \
				&iniContext, 0);
		g_server_stat.total_get_count = iniGetInt64Value(NULL, \
				STAT_ITEM_TOTAL_GET, \
				&iniContext, 0);
		g_server_stat.success_get_count = iniGetInt64Value(NULL, \
				STAT_ITEM_SUCCESS_GET, \
				&iniContext, 0);
		g_server_stat.total_inc_count = iniGetInt64Value(NULL, \
				STAT_ITEM_TOTAL_INC, \
				&iniContext, 0);
		g_server_stat.success_inc_count = iniGetInt64Value(NULL, \
				STAT_ITEM_SUCCESS_INC, \
				&iniContext, 0);
		g_server_stat.total_delete_count = iniGetInt64Value(NULL, \
				STAT_ITEM_TOTAL_DELETE, \
				&iniContext, 0);
		g_server_stat.success_delete_count = iniGetInt64Value(NULL, \
				STAT_ITEM_SUCCESS_DELETE, \
				&iniContext, 0);
		iniFreeContext(&iniContext);
	}
	else
	{
		fdht_last_stat.total_set_count = -1;  //for write to stat file
		fdht_last_stat.success_set_count = -1;
	}

	fdht_stat_fd = open(full_filename, O_WRONLY | O_CREAT, 0644);
	if (fdht_stat_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open stat file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, full_filename, errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return fdht_write_to_stat_file();
}

int start_dl_detect_thread()
{
	int result;
	pthread_attr_t thread_attr;

	if (g_db_dead_lock_detect_interval <= 0)
	{
		return 0;
	}
	
	if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
	{
		return result;
	}

	if ((result = pthread_create(&dld_tid, &thread_attr, \
			bdb_dl_detect_entrance, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
				"create bdb_dl_detect_thread fail, " \
				"error no: %d, error info: %s", \
				__LINE__, result, STRERROR(result));
		return result;
	}

	pthread_attr_destroy(&thread_attr);

	return 0;
}

int fdht_func_init(const char *filename, char *bind_addr, const int addr_size)
{
	int result;
	int *group_ids;
	int group_count;
	int *pGroupId;
	int *pGroupEnd;
	int max_group_id;
	int i;
	DBType db_type = DB_BTREE;
	int page_size = 4 * 1024;
	int64_t nCacheSize;
	char db_file_prefix[DB_FILE_PREFIX_MAX_SIZE];
	char db_filename[DB_FILE_PREFIX_MAX_SIZE+8];

	g_server_start_time = g_current_time;

	result = fdht_load_from_conf_file(filename, bind_addr, \
		addr_size, &group_ids, &group_count, 
		&db_type, &nCacheSize, &page_size, db_file_prefix);
	if (result != 0)
	{
		return result;
	}

	store_init();

	max_group_id = 0;
	pGroupEnd = group_ids + group_count;
	for (pGroupId=group_ids; pGroupId<pGroupEnd; pGroupId++)
	{
		if (*pGroupId > max_group_id)
		{
			max_group_id = *pGroupId;
		}
	}

	g_db_count = max_group_id + 1;
	g_db_list = (StoreHandle **)malloc(sizeof(StoreHandle *) * g_db_count);
	if (g_db_list == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(StoreHandle *) * g_db_count, \
			errno, STRERROR(errno));
		free(group_ids);
		return errno != 0 ? errno : ENOMEM;
	}

	for (i=0; i<g_db_count; i++)
	{
		g_db_list[i] = NULL;
	}

	result = 0;
	for (pGroupId=group_ids; pGroupId<pGroupEnd; pGroupId++)
	{
		snprintf(db_filename, sizeof(db_filename), "%s%03d", \
			db_file_prefix, *pGroupId);
		if (g_store_type == FDHT_STORE_TYPE_BDB)
		{
			if ((result=db_init(&g_db_list[*pGroupId], db_type, \
						nCacheSize, page_size, \
						g_fdht_base_path, db_filename)) != 0)
			{
				break;
			}
		}
		else
		{
			if ((result=mp_init(&g_db_list[*pGroupId], \
						nCacheSize)) != 0)
			{
				break;
			}
		}
	}

	free(group_ids);

	if (result == 0)
	{
		result = fdht_load_stat_from_file();
	}

	if ((result=key_init()) != 0)
	{
		return result;
	}

	return result;
}

void fdht_func_destroy()
{
	int i;

	for (i=0; i<g_db_count; i++)
	{
		if (g_db_list[i] != NULL)
		{
			g_func_destroy_instance(&g_db_list[i]);
		}
	}
	g_func_destroy();

	if (fdht_stat_fd >= 0)
	{
		fdht_write_to_stat_file();
		close(fdht_stat_fd);
		fdht_stat_fd = -1;
	}

	if (g_group_servers != NULL)
	{
		free(g_group_servers);
		g_group_servers = NULL;
	}

	key_destroy();
}

int fdht_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len)
{
	if (ftruncate(fd, 0) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"truncate file \"%s\" to empty fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (lseek(fd, 0, SEEK_SET) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"rewind file \"%s\" to start fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (write(fd, buff, len) != len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (fsync(fd) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"sync file \"%s\" to disk fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return 0;
}

int fdht_write_to_stat_file()
{
	char buff[512];
	int len;

	if (memcmp(&fdht_last_stat, &g_server_stat, sizeof(FDHTServerStat)) == 0)
	{
		return 0;
	}

	memcpy(&fdht_last_stat, &g_server_stat, sizeof(FDHTServerStat));
	len = sprintf(buff, 
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n"  \
		"%s=%"PRId64"\n", \
		STAT_ITEM_TOTAL_SET, g_server_stat.total_set_count, \
		STAT_ITEM_SUCCESS_SET, g_server_stat.success_set_count, \
		STAT_ITEM_TOTAL_GET, g_server_stat.total_get_count, \
		STAT_ITEM_SUCCESS_GET, g_server_stat.success_get_count, \
		STAT_ITEM_TOTAL_INC, g_server_stat.total_inc_count, \
		STAT_ITEM_SUCCESS_INC, g_server_stat.success_inc_count, \
		STAT_ITEM_TOTAL_DELETE, g_server_stat.total_delete_count, \
		STAT_ITEM_SUCCESS_DELETE, g_server_stat.success_delete_count \
	    );

	return fdht_write_to_fd(fdht_stat_fd, \
			fdht_get_stat_filename, NULL, buff, len);
}

int fdht_terminate()
{
	struct nio_thread_data *pThreadData;
	struct nio_thread_data *pDataEnd;
	int quit_sock;

	g_continue_flag = false;

	if (g_store_type == FDHT_STORE_TYPE_BDB)
	{
		pthread_kill(dld_tid, SIGINT);
	}

	if (g_thread_data != NULL)
	{
		pDataEnd = g_thread_data + g_max_threads;
		quit_sock = 0;
		for (pThreadData=g_thread_data; pThreadData<pDataEnd; \
			pThreadData++)
		{
			quit_sock--;
			if (write(pThreadData->pipe_fds[1], &quit_sock, \
					sizeof(quit_sock)) != sizeof(quit_sock))
			{
			}
		}
	}

	return 0;
}

int fdht_stat_file_sync_func(void *args)
{
	return fdht_write_to_stat_file();
}

