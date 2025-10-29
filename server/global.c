/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.fastken.com/ for more detail.
**/

#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "global.h"

volatile bool g_continue_flag = true;

int g_server_port = FDHT_SERVER_DEFAULT_PORT;
int g_max_connections = DEFAULT_MAX_CONNECTONS;
int g_max_threads = 4;
int g_accept_threads = 1;
int g_max_pkg_size = FDHT_MAX_PKG_SIZE;
int g_min_buff_size = FDHT_MIN_BUFF_SIZE;
int g_heart_beat_interval = DEFAULT_NETWORK_TIMEOUT / 2;
bool g_write_to_binlog_flag = true;

int g_thread_count = 0;
int g_sync_wait_usec = DEFAULT_SYNC_WAIT_MSEC;
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
int g_sync_binlog_buff_interval = SYNC_BINLOG_BUFF_DEF_INTERVAL;
TimeInfo g_sync_db_time_base = {TIME_NONE, TIME_NONE};
int g_sync_db_interval = DEFAULT_SYNC_DB_INVERVAL;
bool g_need_clear_expired_data = true;
TimeInfo g_clear_expired_time_base = {TIME_NONE, TIME_NONE};
int g_clear_expired_interval = DEFAULT_CLEAR_EXPIRED_INVERVAL;
int g_db_dead_lock_detect_interval = DEFAULT_DB_DEAD_LOCK_DETECT_INVERVAL;
TimeInfo g_compress_binlog_time_base = {TIME_NONE, TIME_NONE};
int g_compress_binlog_interval = COMPRESS_BINLOG_DEF_INTERVAL;
int g_sync_stat_file_interval = DEFAULT_SYNC_STAT_FILE_INTERVAL;
int g_write_mark_file_freq = FDHT_DEFAULT_SYNC_MARK_FILE_FREQ;

struct timeval g_network_tv = {DEFAULT_NETWORK_TIMEOUT, 0};

FDHTServerStat g_server_stat;

int g_group_count = 0;
FDHTGroupServer *g_group_servers = NULL;
int g_group_server_count = 0;

int g_server_join_time = 0;
bool g_sync_old_done = false;
char g_sync_src_ip_addr[IP_ADDRESS_SIZE] = {0};
int g_sync_src_port = 0;
int g_sync_until_timestamp = 0;
int g_sync_done_timestamp = 0;

int g_allow_ip_count = 0;  /* -1 means match any ip address */
in_addr_64_t *g_allow_ip_addrs = NULL;  /* sorted array, asc order */

time_t g_server_start_time = 0;
int g_store_type = FDHT_STORE_TYPE_BDB;
int g_mpool_init_capacity = FDHT_DEFAULT_MPOOL_INIT_CAPACITY;
double g_mpool_load_factor = FDHT_DEFAULT_MPOOL_LOAD_FACTOR;
int g_mpool_clear_min_interval = FDHT_DEFAULT_MPOOL_CLEAR_MIN_INTEVAL;
int g_mpool_htable_lock_count = FDHT_DEFAULT_MPOOL_HTABLE_LOCK_COUNT;

struct nio_thread_data *g_thread_data = NULL;
int g_thread_stack_size = 1 * 1024 * 1024;
struct fast_task_queue g_free_queue;

