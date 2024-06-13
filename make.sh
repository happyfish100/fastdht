GCC_VERSION=$(gcc -dM -E -  < /dev/null | grep -w __GNUC__ | awk '{print $NF;}')
if [ -z "$GCC_VERSION" ]; then
  echo -e "gcc not found, please install gcc first\n" 1>&2
  exit 2
fi

tmp_src_filename=fdht_check_bits.c
cat <<EOF > $tmp_src_filename
#include <stdio.h>
int main()
{
	printf("%d\n", (int)sizeof(long));
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

TARGET_PREFIX=$DESTDIR/usr/local
TARGET_CONF_PATH=$DESTDIR/etc
TARGET_INIT_PATH=$DESTDIR/etc/init.d

#WITH_LINUX_SERVICE=1
DEBUG_FLAG=1

export CC=gcc
CFLAGS='-Wall'
if [ -n "$GCC_VERSION" ] && [ $GCC_VERSION -ge 7 ]; then
  CFLAGS="$CFLAGS -Wformat-truncation=0 -Wformat-overflow=0"
fi

CFLAGS='$CFLAGS -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE'
if [ "$DEBUG_FLAG" = "1" ]; then
  CFLAGS="$CFLAGS -g -O -DDEBUG_FLAG"
else
  CFLAGS="$CFLAGS -O3"
fi

LIBS=''
uname=`uname`
if [ "$uname" = "Linux" ]; then
  CFLAGS="$CFLAGS"
elif [ "$uname" = "FreeBSD" ]; then
  CFLAGS="$CFLAGS"
elif [ "$uname" = "SunOS" ]; then
  CFLAGS="$CFLAGS -D_THREAD_SAFE"
  LIBS="$LIBS -lsocket -lnsl -lresolv"
elif [ "$uname" = "AIX" ]; then
  CFLAGS="$CFLAGS -D_THREAD_SAFE"
elif [ "$uname" = "HP-UX" ]; then
  CFLAGS="$CFLAGS"
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

if [ $have_pthread -eq 0 ] && [ -f /sbin/ldconfig ]; then
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
      if [ ! -d $TARGET_CONF_PATH ]; then
        mkdir -p $TARGET_CONF_PATH
        cp -f conf/fdhtd.conf $TARGET_CONF_PATH
        cp -f conf/fdht_servers.conf $TARGET_CONF_PATH
        cp -f conf/fdht_client.conf $TARGET_CONF_PATH
      fi
      mkdir -p $TARGET_INIT_PATH
      cp -f init.d/fdhtd $TARGET_INIT_PATH
#      /sbin/chkconfig --add fdhtd
    fi
  fi
fi

