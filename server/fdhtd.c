/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDHT may be copied only under the terms of the GNU General
* Public License V3.  Please visit the FastDHT Home Page 
* http://www.csource.org/ for more detail.
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
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "process_ctrl.h"
#include "logger.h"
#include "fdht_global.h"
#include "global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "sched_thread.h"
#include "fdht_io.h"
#include "work_thread.h"
#include "func.h"
#include "sync.h"
#include "db_recovery.h"
#include "mpool_op.h"

static ScheduleArray scheduleArray;
static pthread_t schedule_tid;

static int fdht_init_schedule();
static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);
static void sigChildHandler(int sig);

static void usage(const char *program)
{
	fprintf(stderr, "Usage: %s <config_file> [start | stop | restart]\n",
		program);
}

int main(int argc, char *argv[])
{
	char *conf_filename;
	char bind_addr[IP_ADDRESS_SIZE];
	int result;
	int sock;
	struct sigaction act;
	char pidFilename[MAX_PATH_SIZE];
	bool stop;
 
	memset(bind_addr, 0, sizeof(bind_addr));
	if (argc < 2)
	{
		usage(argv[0]);
		return 1;
	}

	g_current_time = time(NULL);
	log_init();
	conf_filename = argv[1];

	if ((result=get_base_path_from_conf_file(conf_filename,
		g_fdht_base_path, sizeof(g_fdht_base_path))) != 0)
	{
		log_destroy();
		return result;
	}
	snprintf(pidFilename, sizeof(pidFilename),
		"%s/data/fdhtd.pid", g_fdht_base_path);
	if ((result=process_action(pidFilename, argv[2], &stop)) != 0)
	{
		if (result == EINVAL)
		{
			usage(argv[0]);
		}
		log_destroy();
		return result;
	}
	if (stop)
	{
		log_destroy();
		return 0;
	}

	if ((result=fdht_func_init(conf_filename, bind_addr, \
		sizeof(bind_addr))) != 0)
	{
		log_destroy();
		return result;
	}

	daemon_init(true);
	umask(0);
	
	if (dup2(g_log_context.log_fd, STDOUT_FILENO) < 0 || \
		dup2(g_log_context.log_fd, STDERR_FILENO) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call dup2 fail, errno: %d, error info: %s, " \
			"program exit!", __LINE__, errno, STRERROR(errno));
		g_continue_flag = false;
		log_destroy();
		return errno;
	}

	sock = socketServer(bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		fdht_func_destroy();
		log_destroy();
		return result;
	}

	if ((result=tcpsetserveropt(sock, g_fdht_network_timeout)) != 0)
	{
		fdht_func_destroy();
		log_destroy();
		return result;
	}

	if ((result=write_to_pid_file(pidFilename)) != 0)
	{
		log_destroy();
		return result;
	}

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);

	act.sa_handler = sigUsrHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		fdht_func_destroy();
		log_destroy();
		return errno;
	}

	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		fdht_func_destroy();
		log_destroy();
		return errno;
	}
	
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		fdht_func_destroy();
		log_destroy();
		return errno;
	}

	act.sa_handler = sigQuitHandler;
	if(sigaction(SIGINT, &act, NULL) < 0 || \
		sigaction(SIGTERM, &act, NULL) < 0 || \
		sigaction(SIGQUIT, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		fdht_func_destroy();
		log_destroy();
		return errno;
	}

	act.sa_handler = sigChildHandler;
	if(sigaction(SIGCHLD, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		fdht_func_destroy();
		log_destroy();
		return errno;
	}

	if ((result=fdht_sync_init()) != 0)
	{
		fdht_func_destroy();
		log_destroy();
		return result;
	}

	if (g_store_type == FDHT_STORE_TYPE_BDB)
	{
		if ((result=fdht_db_recovery_init()) != 0)
		{
			fdht_func_destroy();
			log_destroy();
			return result;
		}

		if ((result=start_dl_detect_thread()) != 0)
		{
			g_continue_flag = false;
			fdht_func_destroy();
			log_destroy();
			return result;
		}
	}

	if ((result=work_thread_init()) != 0)
	{
		g_continue_flag = false;
		fdht_func_destroy();
		log_destroy();
		return result;
	}

	if ((result=fdht_init_schedule()) != 0)
	{
		g_continue_flag = false;
		work_thread_destroy();
		fdht_func_destroy();
		log_destroy();
		return result;
	}

	log_set_cache(true);

	fdht_accept_loop(sock);

	work_thread_destroy();

	close(sock);

	while (g_schedule_flag) //waiting for schedule thread exit
	{
		sleep(1);
	}

	fdht_sync_destroy();

	if (g_store_type == FDHT_STORE_TYPE_BDB)
	{
		fdht_memp_trickle_dbs((void *)1);
	}

	fdht_func_destroy();

	logInfo("exit nomally.\n");
	log_destroy();

	delete_pid_file(pidFilename);
	
	return 0;
}

static void sigQuitHandler(int sig)
{
	if (g_continue_flag)
	{
		fdht_terminate();
		pthread_kill(schedule_tid, SIGINT);
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);

		/*
		//printf("free queue count: %d, recv queue count: %d, " \
			"work queue count=%d, send queue count=%d\n", \
			free_queue_count(), recv_queue_count(),  \
			work_queue_count(), send_queue_count());
		fflush(stdout);
		*/
	}
}

static void sigHupHandler(int sig)
{
}

static void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_thread_count, \
		nMoCount, nSuccMoCount);
	*/
}

static void sigChildHandler(int sig)
{
	int status;
	pid_t pid;
	while ((pid=waitpid(0, &status, WNOHANG)) >= 0)
	{
		logDebug("file: "__FILE__", line: %d, " \
			"child process %d exits, status=%d.", \
			__LINE__, pid, status);
	}
}

static int fdht_compress_binlog_func(void *arg)
{
	pid_t pid;
	char *cmd;

	pid = fork();
	if (pid < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"fork fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		return errno;
	}

	if (pid > 0) //parrent proccess
	{
		return 0;
	}

	//child process
	cmd = "/usr/local/bin/fdht_compress";
	if (!fileExists(cmd))
	{
		cmd = "/usr/bin/fdht_compress";
	}
	if (execl(cmd, cmd, g_fdht_base_path, "auto", NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"execl fdht_compress fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
	}

	exit(errno);  //exit child proccess
	return 0;
}

static int fdht_init_schedule()
{
	int entry_count;
	int i;
	ScheduleEntry *pScheduleEntry;

	entry_count = 2;
	if (g_store_type == FDHT_STORE_TYPE_BDB && g_sync_db_interval > 0)
	{
		entry_count++;
	}
	if (g_write_to_binlog_flag)
	{
		entry_count++;
	}
	if (g_compress_binlog_interval > 0 && g_need_clear_expired_data)
	{
		entry_count++;
	}
	if (g_clear_expired_interval > 0)
	{
		if (g_store_type == FDHT_STORE_TYPE_BDB)
		{
			for (i=0; i<g_db_count; i++)
			{
				if (g_db_list[i] != NULL)
				{
					entry_count++;
				}
			}
		}
		else //mpool
		{
			entry_count++;
		}
	}

	scheduleArray.entries = (ScheduleEntry *)malloc( \
				sizeof(ScheduleEntry) * entry_count);
	if (scheduleArray.entries == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(ScheduleEntry) * entry_count, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	pScheduleEntry = scheduleArray.entries;
	scheduleArray.count = entry_count;
	memset(pScheduleEntry, 0, sizeof(ScheduleEntry) * entry_count);

	pScheduleEntry->id = pScheduleEntry - scheduleArray.entries + 1;
	pScheduleEntry->time_base.hour = TIME_NONE;
	pScheduleEntry->time_base.minute = TIME_NONE;
	pScheduleEntry->interval = g_sync_log_buff_interval;
	pScheduleEntry->task_func = log_sync_func;
	pScheduleEntry->func_args = &g_log_context;
	pScheduleEntry++;

	pScheduleEntry->id = pScheduleEntry - scheduleArray.entries + 1;
	pScheduleEntry->time_base.hour = TIME_NONE;
	pScheduleEntry->time_base.minute = TIME_NONE;
	pScheduleEntry->interval = g_sync_stat_file_interval;
	pScheduleEntry->task_func = fdht_stat_file_sync_func;
	pScheduleEntry->func_args = NULL;
	pScheduleEntry++;

	if (g_store_type == FDHT_STORE_TYPE_BDB && g_sync_db_interval > 0)
	{
		pScheduleEntry->id = pScheduleEntry - scheduleArray.entries+1;
		pScheduleEntry->time_base.hour = g_sync_db_time_base.hour;
		pScheduleEntry->time_base.minute = g_sync_db_time_base.minute;
		pScheduleEntry->interval = g_sync_db_interval;
		pScheduleEntry->task_func = fdht_memp_trickle_dbs;
		pScheduleEntry->func_args = NULL;
		pScheduleEntry++;
	}

	if (g_write_to_binlog_flag)
	{
		pScheduleEntry->id = pScheduleEntry - scheduleArray.entries+1;
		pScheduleEntry->time_base.hour = TIME_NONE;
		pScheduleEntry->time_base.minute = TIME_NONE;
		pScheduleEntry->interval = g_sync_binlog_buff_interval;
		pScheduleEntry->task_func = fdht_binlog_sync_func;
		pScheduleEntry->func_args = NULL;
		pScheduleEntry++;
	}

	if (g_compress_binlog_interval > 0)
	{
		pScheduleEntry->id = pScheduleEntry - scheduleArray.entries+1;
		pScheduleEntry->time_base.hour = g_compress_binlog_time_base.hour;
		pScheduleEntry->time_base.minute = g_compress_binlog_time_base.minute;
		pScheduleEntry->interval = g_compress_binlog_interval;
		pScheduleEntry->task_func = fdht_compress_binlog_func;
		pScheduleEntry->func_args = NULL;
		pScheduleEntry++;
	}

	if (g_clear_expired_interval > 0 && g_need_clear_expired_data)
	{
		if (g_store_type == FDHT_STORE_TYPE_BDB)
		{
			for (i=0; i<g_db_count; i++)
			{
				if (g_db_list[i] == NULL)
				{
					continue;
				}

				pScheduleEntry->id = pScheduleEntry - \
					scheduleArray.entries + 1;
				pScheduleEntry->time_base.hour = \
					g_clear_expired_time_base.hour;
				pScheduleEntry->time_base.minute = \
					g_clear_expired_time_base.minute;
				pScheduleEntry->interval = \
					g_clear_expired_interval;
				pScheduleEntry->task_func = \
					db_clear_expired_keys;
				pScheduleEntry->func_args = (void *)(long)i;
				pScheduleEntry++;
			}
		}
		else
		{
			pScheduleEntry->id = pScheduleEntry - \
					scheduleArray.entries + 1;
			pScheduleEntry->time_base.hour = \
					g_clear_expired_time_base.hour;
			pScheduleEntry->time_base.minute = \
					g_clear_expired_time_base.minute;
			pScheduleEntry->interval = \
					g_clear_expired_interval;
			pScheduleEntry->task_func = \
					mp_clear_expired_keys;
			pScheduleEntry->func_args = (void *)g_hash_array;
			pScheduleEntry++;
		}
	}

	return sched_start(&scheduleArray, &schedule_tid, \
		g_thread_stack_size, (bool * volatile)&g_continue_flag);
}

