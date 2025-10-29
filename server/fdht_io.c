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
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/ioevent_loop.h"
#include "fdht_global.h"
#include "fdht_proto.h"
#include "work_thread.h"
#include "global.h"
#include "fdht_io.h"

static void client_sock_read(int sock, const int event, void *arg);
static void client_sock_write(int sock, const int event, void *arg);

void task_finish_clean_up(struct fast_task_info *pTask)
{
	ioevent_detach(&pTask->thread_data->ev_puller, pTask->event.fd);
	close(pTask->event.fd);
	pTask->event.fd = -1;

	if (pTask->event.timer.expires > 0)
	{
		fast_timer_remove(&pTask->thread_data->timer,
			&pTask->event.timer);
		pTask->event.timer.expires = 0;
	}

	free_queue_push(pTask);
}

void recv_notify_read(int sock, const int event, void *arg)
{
#if IOEVENT_USE_URING
    const bool use_iouring = true;
#else
    const bool use_iouring = false;
#endif
	int bytes;
	int incomesock;
	struct nio_thread_data *pThreadData;
	struct fast_task_info *pTask;
	char szClientIp[IP_ADDRESS_SIZE];
	in_addr_64_t client_addr;

	while (1)
	{
		if ((bytes=read(sock, &incomesock, sizeof(incomesock))) < 0)
		{
			if (!(errno == EAGAIN || errno == EWOULDBLOCK))
			{
				logError("file: "__FILE__", line: %d, " \
					"call read failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));
			}

			break;
		}
		else if (bytes == 0)
		{
			break;
		}

		if (incomesock < 0)
		{
			return;
		}

		client_addr = getPeerIpaddr(incomesock, \
				szClientIp, IP_ADDRESS_SIZE);
		if (g_allow_ip_count >= 0)
		{
			if (bsearch(&client_addr, g_allow_ip_addrs, \
					g_allow_ip_count, sizeof(in_addr_t), \
					cmp_by_ip_addr_t) == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"ip addr %s is not allowed to access", \
					__LINE__, szClientIp);

				close(incomesock);
				continue;
			}
		}

		if (tcpsetnonblockopt(incomesock) != 0)
		{
			close(incomesock);
			continue;
		}

		pTask = free_queue_pop(&g_free_queue);
		if (pTask == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc task buff failed", \
				__LINE__);
			close(incomesock);
			continue;
		}

		strcpy(pTask->client_ip, szClientIp);
		pThreadData = g_thread_data + incomesock % g_max_threads;
		if (ioevent_set(pTask, pThreadData, incomesock, IOEVENT_READ,
			client_sock_read, g_fdht_network_timeout, use_iouring) != 0)
		{
			task_finish_clean_up(pTask);
			continue;
		}
	}
}

static int set_send_event(struct fast_task_info *pTask)
{
	int result;

	if (pTask->event.callback == client_sock_write)
	{
		return 0;
	}

	pTask->event.callback = client_sock_write;
	if (ioevent_modify(&pTask->thread_data->ev_puller,
		pTask->event.fd, IOEVENT_WRITE, pTask) != 0)
	{
		result = errno != 0 ? errno : ENOENT;
		task_finish_clean_up(pTask);

		logError("file: "__FILE__", line: %d, "\
			"ioevent_modify fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}
	return 0;
}

int send_add_event(struct fast_task_info *pTask)
{
	pTask->send.ptr->offset = 0;

	/* direct send */
	client_sock_write(pTask->event.fd, IOEVENT_WRITE, pTask);
	return 0;
}

static void client_sock_read(int sock, const int event, void *arg)
{
	int bytes;
	int recv_bytes;
	struct fast_task_info *pTask;

	pTask = (struct fast_task_info *)arg;

	if (event & IOEVENT_TIMEOUT)
	{
		if (pTask->send.ptr->offset == 0 && \
			((FDHTProtoHeader *)pTask->send.ptr->data)->keep_alive)
		{
			pTask->event.timer.expires = g_current_time +
				g_fdht_network_timeout;
			fast_timer_add(&pTask->thread_data->timer,
				&pTask->event.timer);
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv timeout, " \
				"recv offset: %d, expect length: %d", \
				__LINE__, pTask->client_ip, \
				pTask->send.ptr->offset, pTask->send.ptr->length);

			task_finish_clean_up(pTask);
		}

		return;
	}

	if (event & IOEVENT_ERROR)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv error event: %d, "
			"close connection", __LINE__, pTask->client_ip, event);

		task_finish_clean_up(pTask);
		return;
	}

	while (1)
	{
		fast_timer_modify(&pTask->thread_data->timer,
			&pTask->event.timer, g_current_time +
			g_fdht_network_timeout);
		if (pTask->send.ptr->length == 0) //recv header
		{
			recv_bytes = sizeof(FDHTProtoHeader) - pTask->send.ptr->offset;
		}
		else
		{
			recv_bytes = pTask->send.ptr->length - pTask->send.ptr->offset;
		}

		if (pTask->send.ptr->offset + recv_bytes > pTask->send.ptr->size)
		{
			char *pTemp;

			pTemp = pTask->send.ptr->data;
			pTask->send.ptr->data = realloc(pTask->send.ptr->data, \
				pTask->send.ptr->size + recv_bytes);
			if (pTask->send.ptr->data == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"malloc failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, STRERROR(errno));

				pTask->send.ptr->data = pTemp;  //restore old data

				task_finish_clean_up(pTask);
				return;
			}

			pTask->send.ptr->size += recv_bytes;
		}

		bytes = recv(sock, pTask->send.ptr->data + pTask->send.ptr->offset, recv_bytes, 0);
		if (bytes < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, recv failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->client_ip, \
					errno, STRERROR(errno));
				task_finish_clean_up(pTask);
			}

			return;
		}
		else if (bytes == 0)
		{
			logDebug("file: "__FILE__", line: %d, " \
				"client ip: %s, recv failed, " \
				"connection disconnected.", \
				__LINE__, pTask->client_ip);
			task_finish_clean_up(pTask);
			return;
		}

		if (pTask->send.ptr->length == 0) //header
		{
			if (pTask->send.ptr->offset + bytes < sizeof(FDHTProtoHeader))
			{
				pTask->send.ptr->offset += bytes;
				return;
			}

			pTask->send.ptr->length = buff2int(((FDHTProtoHeader *) \
						pTask->send.ptr->data)->pkg_len);
			if (pTask->send.ptr->length < 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, pkg length: %d < 0", \
					__LINE__, pTask->client_ip, \
					pTask->send.ptr->length);
				task_finish_clean_up(pTask);
				return;
			}

			pTask->send.ptr->length += sizeof(FDHTProtoHeader);
			if (pTask->send.ptr->length > g_max_pkg_size)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, pkg length: %d > " \
					"max pkg size: %d", __LINE__, \
					pTask->client_ip, pTask->send.ptr->length, \
					g_max_pkg_size);
				task_finish_clean_up(pTask);
				return;
			}
		}

		pTask->send.ptr->offset += bytes;
		if (pTask->send.ptr->offset >= pTask->send.ptr->length) //recv done
		{
			work_deal_task(pTask);
			return;
		}
	}

	return;
}

static void client_sock_write(int sock, const int event, void *arg)
{
	int bytes;
	int result;
	struct fast_task_info *pTask;

	pTask = (struct fast_task_info *)arg;
	if (event & IOEVENT_TIMEOUT)
	{
		logError("file: "__FILE__", line: %d, " \
			"send timeout", __LINE__);

		task_finish_clean_up(pTask);
		return;
	}

	if (event & IOEVENT_ERROR)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv error event: %d, "
			"close connection", __LINE__, pTask->client_ip, event);

		task_finish_clean_up(pTask);
		return;
	}

	while (1)
	{
		fast_timer_modify(&pTask->thread_data->timer,
			&pTask->event.timer, g_current_time +
			g_fdht_network_timeout);
		bytes = send(sock, pTask->send.ptr->data + pTask->send.ptr->offset, \
				pTask->send.ptr->length - pTask->send.ptr->offset,  0);
		//printf("%08X sended %d bytes\n", (int)pTask, bytes);
		if (bytes < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				set_send_event(pTask);
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, recv failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->client_ip, \
					errno, STRERROR(errno));

				task_finish_clean_up(pTask);
			}

			return;
		}
		else if (bytes == 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"send failed, connection disconnected.", \
				__LINE__);

			task_finish_clean_up(pTask);
			return;
		}

		pTask->send.ptr->offset += bytes;
		if (pTask->send.ptr->offset >= pTask->send.ptr->length)
		{
			if (((FDHTProtoHeader *)pTask->send.ptr->data)->keep_alive)
			{
				pTask->send.ptr->offset = 0;
				pTask->send.ptr->length  = 0;

				pTask->event.callback = client_sock_read;
				if (ioevent_modify(&pTask->thread_data->ev_puller,
					pTask->event.fd, IOEVENT_READ, pTask) != 0)
				{
					result = errno != 0 ? errno : ENOENT;
					task_finish_clean_up(pTask);

					logError("file: "__FILE__", line: %d, "\
						"ioevent_modify fail, " \
						"errno: %d, error info: %s", \
						__LINE__, result, STRERROR(result));
					return;
				}
			}
			else
			{
				task_finish_clean_up(pTask);
			}

			return;
		}
	}
}

