#ifndef FASTDHT_CLIENT_H
#define FASTDHT_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef PHP_WIN32
#define PHP_FASTDHT_API __declspec(dllexport)
#else
#define PHP_FASTDHT_API
#endif

PHP_MINIT_FUNCTION(fastdht_client);
PHP_RINIT_FUNCTION(fastdht_client);
PHP_MSHUTDOWN_FUNCTION(fastdht_client);
PHP_RSHUTDOWN_FUNCTION(fastdht_client);
PHP_MINFO_FUNCTION(fastdht_client);

ZEND_FUNCTION(fastdht_set);
ZEND_FUNCTION(fastdht_get);
ZEND_FUNCTION(fastdht_inc);
ZEND_FUNCTION(fastdht_delete);
ZEND_FUNCTION(fastdht_batch_set);
ZEND_FUNCTION(fastdht_batch_get);
ZEND_FUNCTION(fastdht_batch_delete);
ZEND_FUNCTION(fastdht_stat);
ZEND_FUNCTION(fastdht_stat_all);
ZEND_FUNCTION(fastdht_get_sub_keys);

PHP_FASTDHT_API zend_class_entry *php_fdht_get_ce(void);
PHP_FASTDHT_API zend_class_entry *php_fdht_get_exception(void);
PHP_FASTDHT_API zend_class_entry *php_fdht_get_exception_base(int root TSRMLS_DC);

#ifdef __cplusplus
}
#endif

#endif	/* FASTDHT_CLIENT_H */
