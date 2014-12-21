tmp_src_filename=fdht_check_bits.c
cat <<EOF > $tmp_src_filename
#include <stdio.h>
int main()
{
	printf("%d\n", sizeof(long));
	return 0;
}
EOF

gcc -D_FILE_OFFSET_BITS=64 -o a.out $tmp_src_filename
bytes=`./a.out`

/bin/rm -f  a.out $tmp_src_filename
if [ "$bytes" -eq 8 ]; then
 OS_BITS=64
else
 OS_BITS=32
fi

cat <<EOF > common/_os_bits.h
#ifndef _OS_BITS_H
#define _OS_BITS_H

#define OS_BITS  $OS_BITS

#endif
EOF

TARGET_PREFIX=/usr/local
TARGET_CONF_PATH=/etc/fdht

#WITH_LINUX_SERVICE=1
DEBUG_FLAG=1

CFLAGS='-Wall -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE'
if [ "$DEBUG_FLAG" = "1" ]; then
  CFLAGS="$CFLAGS -g -O -DDEBUG_FLAG"
else
  CFLAGS="$CFLAGS -O3"
fi

LIBS=''
uname=`uname`
if [ "$uname" = "Linux" ]; then
  CFLAGS="$CFLAGS -DOS_LINUX -DIOEVENT_USE_EPOLL"
elif [ "$uname" = "FreeBSD" ]; then
  CFLAGS="$CFLAGS -DOS_FREEBSD -DIOEVENT_USE_KQUEUE"
elif [ "$uname" = "SunOS" ]; then
  CFLAGS="$CFLAGS -DOS_SUNOS -D_THREAD_SAFE -DIOEVENT_USE_PORT"
  LIBS="$LIBS -lsocket -lnsl -lresolv"
  export CC=gcc
elif [ "$uname" = "AIX" ]; then
  CFLAGS="$CFLAGS -DOS_AIX -D_THREAD_SAFE"
  export CC=gcc
elif [ "$uname" = "HP-UX" ]; then
  CFLAGS="$CFLAGS -DOS_HPUX"
fi

have_pthread=0
if [ -f /usr/lib/libpthread.so ] || [ -f /usr/local/lib/libpthread.so ] || [ -f /lib64/libpthread.so ] || [ -f /usr/lib64/libpthread.so ] || [ -f /usr/lib/libpthread.a ] || [ -f /usr/local/lib/libpthread.a ] || [ -f /lib64/libpthread.a ] || [ -f /usr/lib64/libpthread.a ]; then
  LIBS="$LIBS -lpthread"
  have_pthread=1
elif [ "$uname" = "HP-UX" ]; then
  lib_path="/usr/lib/hpux$OS_BITS"
  if [ -f $lib_path/libpthread.so ]; then
    LIBS="-L$lib_path -lpthread"
    have_pthread=1
  fi
elif [ "$uname" = "FreeBSD" ]; then
  if [ -f /usr/lib/libc_r.so ]; then
    line=$(nm -D /usr/lib/libc_r.so | grep pthread_create | grep -w T)
    if [ $? -eq 0 ]; then
      LIBS="$LIBS -lc_r"
      have_pthread=1
    fi
  elif [ -f /lib64/libc_r.so ]; then
    line=$(nm -D /lib64/libc_r.so | grep pthread_create | grep -w T)
    if [ $? -eq 0 ]; then
      LIBS="$LIBS -lc_r"
      have_pthread=1
    fi
  elif [ -f /usr/lib64/libc_r.so ]; then
    line=$(nm -D /usr/lib64/libc_r.so | grep pthread_create | grep -w T)
    if [ $? -eq 0 ]; then
      LIBS="$LIBS -lc_r"
      have_pthread=1
    fi
  fi
fi

if [ $have_pthread -eq 0 ]; then
   /sbin/ldconfig -p | fgrep libpthread.so > /dev/null
   if [ $? -eq 0 ]; then
      LIBS="$LIBS -lpthread"
   else
      echo -E 'Require pthread lib, please check!'
      exit 2
   fi
fi

cd server
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PREFIX\)#$TARGET_PREFIX#g" Makefile
perl -pi -e "s#\\\$\(TARGET_CONF_PATH\)#$TARGET_CONF_PATH#g" Makefile
make $1 $2

cd ../tool 
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PREFIX\)#$TARGET_PREFIX#g" Makefile
make $1 $2

cd ../client
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PREFIX\)#$TARGET_PREFIX#g" Makefile
perl -pi -e "s#\\\$\(TARGET_CONF_PATH\)#$TARGET_CONF_PATH#g" Makefile
make $1 $2

#cd test
#cp Makefile.in Makefile
#perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
#perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
#perl -pi -e "s#\\\$\(TARGET_PREFIX\)#$TARGET_PREFIX#g" Makefile
#cd ..

if [ "$1" = "install" ]; then
  cd ..
  cp -f restart.sh $TARGET_PREFIX/bin
  cp -f stop.sh $TARGET_PREFIX/bin

  if [ "$uname" = "Linux" ]; then
    if [ "$WITH_LINUX_SERVICE" = "1" ]; then
      if [ ! -d /etc/fdht ]; then
        mkdir -p /etc/fdht
        cp -f conf/fdhtd.conf /etc/fdht/
        cp -f conf/fdht_servers.conf /etc/fdht/
        cp -f conf/fdht_client.conf /etc/fdht/
      fi

      cp -f init.d/fdhtd /etc/rc.d/init.d/
      /sbin/chkconfig --add fdhtd
    fi
  fi
fi

