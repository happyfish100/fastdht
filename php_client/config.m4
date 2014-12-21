dnl config.m4 for extension fastdht_client

PHP_ARG_WITH(fastdht_client, for fastdht_client support FastDHT client,
[  --with-fastdht_client             Include fastdht_client support FastDHT client])

if test "$PHP_FASTDHT_CLIENT" != "no"; then
  PHP_SUBST(FASTDHT_CLIENT_SHARED_LIBADD)

  if test -z "$ROOT"; then
	ROOT=/usr/local
  fi

dnl  CC=$ROOT/bin/gcc

  PHP_ADD_INCLUDE($ROOT/include)
  PHP_ADD_INCLUDE($ROOT/include/fastcommon)
  PHP_ADD_INCLUDE($ROOT/include/fastdht)

  PHP_ADD_LIBRARY_WITH_PATH(fastcommon, $ROOT/lib, FASTDHT_CLIENT_SHARED_LIBADD)
  PHP_ADD_LIBRARY_WITH_PATH(fdhtclient, $ROOT/lib, FASTDHT_CLIENT_SHARED_LIBADD)

  PHP_NEW_EXTENSION(fastdht_client, fastdht_client.c, $ext_shared)

  CFLAGS="$CFLAGS -Werror -Wall"
fi
