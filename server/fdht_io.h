/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_io.h

#ifndef _FDHT_IO_H
#define _FDHT_IO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "fdht_define.h"

#ifdef __cplusplus
extern "C" {
#endif

void recv_notify_read(int sock, const int event, void *arg);
int send_add_event(struct fast_task_info *pTask);
void task_finish_clean_up(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

