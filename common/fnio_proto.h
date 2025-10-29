/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.fastken.com/ for more detail.
**/

//fnio_proto.h

#ifndef _FNIO_PROTO_H_
#define _FNIO_PROTO_H_

#include "fastcommon/common_define.h"

#define FNIO_PROTO_SOURCE_PROXY  'P'
#define FNIO_PROTO_SOURCE_DATA   'D'


#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	char szIpAddrLen[4];
	char ip_addr[IP_ADDRESS_SIZE];
	char szPort[4];
} FNIOProtoServerInfo;

typedef struct {
	char pkg_len[4];
	char session_id[8];
	char source;
} FNIOProtoHeader;

#ifdef __cplusplus
}
#endif

#endif

