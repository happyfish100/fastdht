/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.fastken.com/ for more detail.
**/

//db_recovery.h

#ifndef _DB_RECOVERY_H
#define _DB_RECOVERY_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"
#include "db_op.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdht_db_recovery_init();
int fdht_memp_trickle_dbs(void *args);

#ifdef __cplusplus
}
#endif

#endif

