#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <php.h>

#ifdef ZTS
#include "TSRM.h"
#endif

#include <SAPI.h>
#include <php_ini.h>
#include "ext/standard/info.h"
#include <zend_extensions.h>
#include <zend_exceptions.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <fdht_client.h>
#include <fdht_func.h>
#include <logger.h>
#include "fdht_global.h"
#include "shared_func.h"
#include "fastdht_client.h"

#define FDHT_STAT_MAX_ROWS   32

typedef struct
{
        GroupArray *pGroupArray;
        bool keep_alive;
} FDHTConfigInfo;

typedef struct
{
        zend_object zo;
        FDHTConfigInfo *pConfigInfo;
        GroupArray *pGroupArray;
} php_fdht_t;

static FDHTConfigInfo *config_list = NULL;
static int config_count = 0;

static int le_fdht;

static zend_class_entry *fdht_ce = NULL;
static zend_class_entry *fdht_exception_ce = NULL;

#if HAVE_SPL
static zend_class_entry *spl_ce_RuntimeException = NULL;
#endif

#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION < 3)
const zend_fcall_info empty_fcall_info = { 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, 0 };
#undef ZEND_BEGIN_ARG_INFO_EX
#define ZEND_BEGIN_ARG_INFO_EX(name, pass_rest_by_reference, return_reference, required_num_args) \
    static zend_arg_info name[] = {                                                               \
        { NULL, 0, NULL, 0, 0, 0, pass_rest_by_reference, return_reference, required_num_args },
#endif

#define TRIM_PHP_KEY(szKey, key_len)  \
	if (key_len > 0 && *(szKey + (key_len - 1)) == '\0') \
	{ \
		key_len--; \
	} \

// Every user visible function must have an entry in fastdht_client_functions[].
	zend_function_entry fastdht_client_functions[] = {
		ZEND_FE(fastdht_set, NULL)
		ZEND_FE(fastdht_get, NULL)
		ZEND_FE(fastdht_inc, NULL)
		ZEND_FE(fastdht_delete, NULL)
		ZEND_FE(fastdht_batch_set, NULL)
		ZEND_FE(fastdht_batch_get, NULL)
		ZEND_FE(fastdht_batch_delete, NULL)
		ZEND_FE(fastdht_stat, NULL)
		ZEND_FE(fastdht_stat_all, NULL)
		ZEND_FE(fastdht_get_sub_keys, NULL)
		{NULL, NULL, NULL}  /* Must be the last line */
	};

zend_module_entry fastdht_client_module_entry = {
	STANDARD_MODULE_HEADER,
	"fastdht_client",
	fastdht_client_functions,
	PHP_MINIT(fastdht_client),
	PHP_MSHUTDOWN(fastdht_client),
	NULL,//PHP_RINIT(fastdht_client),
	NULL,//PHP_RSHUTDOWN(fastdht_client),
	PHP_MINFO(fastdht_client),
	"1.06", 
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_FASTDHT_CLIENT
	ZEND_GET_MODULE(fastdht_client)
#endif

#define FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey) \
	if (key_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		key_info.namespace_len = FDHT_MAX_NAMESPACE_LEN; \
	} \
	if (key_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		key_info.obj_id_len = FDHT_MAX_OBJECT_ID_LEN; \
	} \
	if (key_info.key_len > FDHT_MAX_SUB_KEY_LEN) \
	{ \
		key_info.key_len = FDHT_MAX_SUB_KEY_LEN; \
	} \
 \
	memcpy(key_info.szNameSpace, szNamespace, key_info.namespace_len); \
	memcpy(key_info.szObjectId, szObjectId, key_info.obj_id_len); \
	memcpy(key_info.szKey, szKey, key_info.key_len); \


#define FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId) \
	if (obj_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		obj_info.namespace_len = FDHT_MAX_NAMESPACE_LEN; \
	} \
	if (obj_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		obj_info.obj_id_len = FDHT_MAX_OBJECT_ID_LEN; \
	} \
 \
	memcpy(obj_info.szNameSpace, szNamespace, obj_info.namespace_len); \
	memcpy(obj_info.szObjectId, szObjectId, obj_info.obj_id_len); \


static void php_fdht_set_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char *szValue;
	int value_len;
	long expires;
	FDHTKeyInfo key_info;

	argc = ZEND_NUM_ARGS();
	if (argc != 4 && argc != 5)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameters count: %d != 4 or 5", 
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssss|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &szValue, &value_len, &expires)
		 == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"szValue=%s(%d), expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len,
		szValue, value_len, expires);
	*/

	RETURN_LONG(fdht_set_ex(pGroupArray, bKeepAlive, &key_info, expires, \
			szValue, value_len));
}

/*
int fastdht_set(string namespace, string object_id, string key, 
		string value [, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_set)
{
	php_fdht_set_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

static void php_fdht_batch_set_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	zval *key_values;
	HashTable *key_value_hash;
	zval **data;
	zval ***ppp;
	int key_count;
	int success_count;
	char *szKey;
	ulong index;
	long expires;
	int result;
	FDHTObjectInfo obj_info;
	FDHTKeyValuePair key_list[FDHT_MAX_KEY_COUNT_PER_REQ];
	zval zvalues[FDHT_MAX_KEY_COUNT_PER_REQ];
	zval *pValue;
	zval *pValueEnd;
	FDHTKeyValuePair *pKeyValue;
	FDHTKeyValuePair *pKeyValueEnd;
	HashPosition pointer;

	argc = ZEND_NUM_ARGS();
	if (argc != 3 && argc != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameters count: %d != 3 or 4", \
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssa|l", &szNamespace, 
		&obj_info.namespace_len, &szObjectId, &obj_info.obj_id_len, 
		&key_values, &expires) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_set parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	key_value_hash = Z_ARRVAL_P(key_values);
	key_count = zend_hash_num_elements(key_value_hash);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_set, invalid key_count: %d!", \
			__LINE__, key_count);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), "
		"expires=%ld", szNamespace, obj_info.namespace_len, 
		szObjectId, obj_info.obj_id_len, expires);
	*/

	memset(zvalues, 0, sizeof(zvalues));
	memset(key_list, 0, sizeof(key_list));
	pValue = zvalues;
	pKeyValue = key_list;
	ppp = &data;
	for (zend_hash_internal_pointer_reset_ex(key_value_hash, &pointer);
	     zend_hash_get_current_data_ex(key_value_hash, (void **)ppp,
		&pointer) == SUCCESS; zend_hash_move_forward_ex(
		key_value_hash, &pointer))
	{
		if (zend_hash_get_current_key_ex(key_value_hash, &szKey, \
			(uint *)&(pKeyValue->key_len), &index, 0, \
			&pointer) != HASH_KEY_IS_STRING)
		{
			logError("file: "__FILE__", line: %d, " \
				"fastdht_batch_set, invalid array element, " \
				"index=%d!", __LINE__, index);
			RETURN_LONG(EINVAL);
		}

		TRIM_PHP_KEY(szKey, pKeyValue->key_len)

		if (pKeyValue->key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			pKeyValue->key_len = FDHT_MAX_SUB_KEY_LEN;
		}
		memcpy(pKeyValue->szKey, szKey, pKeyValue->key_len);

		if (Z_TYPE_PP(data) == IS_STRING)
		{
			pKeyValue->pValue = Z_STRVAL_PP(data);
			pKeyValue->value_len = Z_STRLEN_PP(data);
		}
		else
		{
			*pValue = **data;
			zval_copy_ctor(pValue);
			convert_to_string(pValue);
			pKeyValue->pValue = Z_STRVAL(*pValue);
			pKeyValue->value_len = Z_STRLEN(*pValue);

			pValue++;
		}

		/*
		logInfo("key=%s(%d), value=%s(%d)", \
			pKeyValue->szKey, pKeyValue->key_len, \
			pKeyValue->pValue, pKeyValue->value_len);
		*/

		pKeyValue++;
	}
	pValueEnd = pValue;

	result = fdht_batch_set_ex(pGroupArray, bKeepAlive, &obj_info, \
			key_list, key_count, expires, &success_count);
	for (pValue=zvalues; pValue<pValueEnd; pValue++)
	{
		zval_dtor(pValue);
	}

	if (result != 0)
	{
		RETURN_LONG(result);
	}

	if (success_count == key_count)
	{
		RETURN_LONG(result);
	}

	array_init(return_value);

	pKeyValueEnd = key_list + key_count;
	for (pKeyValue=key_list; pKeyValue<pKeyValueEnd; pKeyValue++)
	{
		add_assoc_long_ex(return_value, pKeyValue->szKey, \
				pKeyValue->key_len + 1, pKeyValue->status);
	}
}

/*
int fastdht_batch_set(string namespace, string object_id, array key_list, 
		[, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_batch_set)
{
	php_fdht_batch_set_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_group_array, g_keep_alive);
}

static void php_fdht_batch_get_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	zval *key_values;
	HashTable *key_value_hash;
	zval **data;
	zval ***ppp;
	int key_count;
	int success_count;
	char *szKey;
	ulong index;
	bool return_errno;
	long expires;
	int result;
	FDHTObjectInfo obj_info;
	FDHTKeyValuePair key_list[FDHT_MAX_KEY_COUNT_PER_REQ];
	FDHTKeyValuePair *pKeyValue;
	FDHTKeyValuePair *pKeyValueEnd;
	HashPosition pointer;

	argc = ZEND_NUM_ARGS();
	if (argc != 3 && argc != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_get parameters count: %d != 3 or 4", \
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssa|bl", &szNamespace, 
		&obj_info.namespace_len, &szObjectId, &obj_info.obj_id_len, 
		&key_values, &return_errno, &expires) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_get parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	key_value_hash = Z_ARRVAL_P(key_values);
	key_count = zend_hash_num_elements(key_value_hash);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_get, invalid key_count: %d!", \
			__LINE__, key_count);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), "
		"expires=%ld", szNamespace, obj_info.namespace_len, 
		szObjectId, obj_info.obj_id_len, expires);
	*/

	memset(key_list, 0, sizeof(key_list));
	pKeyValue = key_list;
	ppp = &data;
	for (zend_hash_internal_pointer_reset_ex(key_value_hash, &pointer);
	     zend_hash_get_current_data_ex(key_value_hash, (void **)ppp,
		&pointer) == SUCCESS; zend_hash_move_forward_ex(
		key_value_hash, &pointer))
	{
		if (zend_hash_get_current_key_ex(key_value_hash, &szKey, \
			(uint *)&(pKeyValue->key_len), &index, 0, \
			&pointer) == HASH_KEY_IS_STRING)
		{
			TRIM_PHP_KEY(szKey, pKeyValue->key_len)
		}
		else if (Z_TYPE_PP(data) == IS_STRING)
		{
			szKey = Z_STRVAL_PP(data);
			pKeyValue->key_len = Z_STRLEN_PP(data);
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"fastdht_batch_get, invalid array element, "\
				"index=%d!", __LINE__, index);
			RETURN_LONG(EINVAL);
		}

		if (pKeyValue->key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			pKeyValue->key_len = FDHT_MAX_SUB_KEY_LEN;
		}
		memcpy(pKeyValue->szKey, szKey, pKeyValue->key_len);

		/*
		logInfo("key=%s(%d)", \
			pKeyValue->szKey, pKeyValue->key_len);
		*/

		pKeyValue++;
	}

	result = fdht_batch_get_ex1(pGroupArray, bKeepAlive, \
			&obj_info, key_list, key_count, expires, \
			_emalloc, &success_count);
	if (result != 0)
	{
		if (return_errno)
		{
			RETURN_LONG(result);
		}
		else
		{
			RETURN_BOOL(false);
		}
	}

	array_init(return_value);

	pKeyValueEnd = key_list + key_count;
	for (pKeyValue=key_list; pKeyValue<pKeyValueEnd; pKeyValue++)
	{
		if (pKeyValue->status == 0)
		{
			add_assoc_stringl_ex(return_value, pKeyValue->szKey, \
				pKeyValue->key_len + 1, pKeyValue->pValue, \
				pKeyValue->value_len, 0);
		}
		else
		{
			if (return_errno)
			{
			add_assoc_long_ex(return_value, pKeyValue->szKey, \
				pKeyValue->key_len + 1, pKeyValue->status);
			}
			else
			{
			add_assoc_bool_ex(return_value, pKeyValue->szKey, \
				pKeyValue->key_len + 1, false);
			}
		}
	}
}

/*
array/int/boolean fastdht_batch_get(string namespace, string object_id, \
		array key_list, [, bool return_errno, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_batch_get)
{
	php_fdht_batch_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_group_array, g_keep_alive);
}

static void php_fdht_get_sub_keys_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	bool return_errno;
	char sub_keys[FDHT_KEY_LIST_MAX_SIZE];
	char **ppKey;
	char **ppEnd;
	int result;
	int key_count;
	FDHTObjectInfo obj_info;
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];

	argc = ZEND_NUM_ARGS();
	if (argc != 2 && argc != 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_get_sub_keys parameters count: %d != 2 or 3", \
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	if (zend_parse_parameters(argc TSRMLS_CC, "ss|b", &szNamespace, 
		&obj_info.namespace_len, &szObjectId, &obj_info.obj_id_len, 
		&return_errno) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_get parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), "
		"expires=%ld", szNamespace, obj_info.namespace_len, 
		szObjectId, obj_info.obj_id_len, expires);
	*/

	result = fdht_get_sub_keys_ex(pGroupArray, bKeepAlive, \
			&obj_info, sub_keys, sizeof(sub_keys));
	if (result != 0)
	{
		if (return_errno)
		{
			RETURN_LONG(result);
		}
		else
		{
			RETURN_BOOL(false);
		}
	}

	array_init(return_value);

	key_count = splitEx(sub_keys, FDHT_FULL_KEY_SEPERATOR, \
			key_array, FDHT_KEY_LIST_MAX_COUNT);
	ppEnd = key_array + key_count;
	for (ppKey=key_array; ppKey<ppEnd; ppKey++)
	{
		add_next_index_string(return_value, *ppKey, 1);
	}
}

/*
array/int/boolean fastdht_get_sub_keys(string namespace, string object_id, \
		[, bool return_errno])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_get_sub_keys)
{
	php_fdht_get_sub_keys_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_group_array, g_keep_alive);
}

static void php_fdht_batch_delete_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	zval *key_values;
	HashTable *key_value_hash;
	zval **data;
	zval ***ppp;
	int key_count;
	int success_count;
	char *szKey;
	ulong index;
	int result;
	FDHTObjectInfo obj_info;
	FDHTKeyValuePair key_list[FDHT_MAX_KEY_COUNT_PER_REQ];
	FDHTKeyValuePair *pKeyValue;
	FDHTKeyValuePair *pKeyValueEnd;
	HashPosition pointer;

	argc = ZEND_NUM_ARGS();
	if (argc != 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_delete parameters count: %d != 3", \
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	if (zend_parse_parameters(argc TSRMLS_CC, "ssa", &szNamespace, 
		&obj_info.namespace_len, &szObjectId, &obj_info.obj_id_len, 
		&key_values) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_delete parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	key_value_hash = Z_ARRVAL_P(key_values);
	key_count = zend_hash_num_elements(key_value_hash);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_delete, invalid key_count: %d!", \
			__LINE__, key_count);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), "
		"expires=%ld", szNamespace, obj_info.namespace_len, 
		szObjectId, obj_info.obj_id_len, expires);
	*/

	memset(key_list, 0, sizeof(key_list));
	pKeyValue = key_list;
	ppp = &data;
	for (zend_hash_internal_pointer_reset_ex(key_value_hash, &pointer);
	     zend_hash_get_current_data_ex(key_value_hash, (void **)ppp,
		&pointer) == SUCCESS; zend_hash_move_forward_ex(
		key_value_hash, &pointer))
	{
		if (zend_hash_get_current_key_ex(key_value_hash, &szKey, \
			(uint *)&(pKeyValue->key_len), &index, 0, \
			&pointer) == HASH_KEY_IS_STRING)
		{
			TRIM_PHP_KEY(szKey, pKeyValue->key_len)
		}
		else if (Z_TYPE_PP(data) == IS_STRING)
		{
			szKey = Z_STRVAL_PP(data);
			pKeyValue->key_len = Z_STRLEN_PP(data);
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"fastdht_batch_delete, invalid array element, "\
				"index=%d!", __LINE__, index);
			RETURN_LONG(EINVAL);
		}

		if (pKeyValue->key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			pKeyValue->key_len = FDHT_MAX_SUB_KEY_LEN;
		}
		memcpy(pKeyValue->szKey, szKey, pKeyValue->key_len);

		/*
		logInfo("key=%s(%d)", 
			pKeyValue->szKey, pKeyValue->key_len);
		*/

		pKeyValue++;
	}

	result = fdht_batch_delete_ex(pGroupArray, bKeepAlive, &obj_info, \
				key_list, key_count, &success_count);
	if (result != 0)
	{
		RETURN_LONG(result);
	}

	if (success_count == key_count)
	{
		RETURN_LONG(result);
	}

	array_init(return_value);

	pKeyValueEnd = key_list + key_count;
	for (pKeyValue=key_list; pKeyValue<pKeyValueEnd; pKeyValue++)
	{
		add_assoc_long_ex(return_value, pKeyValue->szKey, \
				pKeyValue->key_len + 1, pKeyValue->status);
	}
}

/*
int fastdht_batch_delete(string namespace, string object_id, array key_list)
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_batch_delete)
{
	php_fdht_batch_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_group_array, g_keep_alive);
}

static void php_fdht_get_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char *pValue;
	int value_len;
	bool return_errno;
	long expires;
	int result;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc < 3  || argc > 5)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_get parameters: %d != 3 or 4 or 5", \
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	expires = FDHT_EXPIRES_NONE;
	if (zend_parse_parameters(argc TSRMLS_CC, "sss|bl", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &return_errno, &expires)
		 == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_get parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len, expires);
	*/

	pValue = NULL;
	value_len = 0;
	if ((result=fdht_get_ex1(pGroupArray, bKeepAlive, &key_info, \
			expires, &pValue, &value_len, _emalloc)) != 0)
	{
		if (return_errno)
		{
			RETURN_LONG(result);
		}
		else
		{
			RETURN_BOOL(false);
		}
	}

	RETURN_STRINGL(pValue, value_len, 0);
}

/*
string/int/boolean fastdht_get(string namespace, string object_id, string key
		[bool return_errno, int expires])
return string value for success, int value (errno) for error
*/
ZEND_FUNCTION(fastdht_get)
{
	php_fdht_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

static void php_fdht_inc_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char szValue[32];
	int value_len;
	long increment;
	bool return_errno;
	long expires;
	int result;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc != 4 && argc != 5 && argc != 6)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_inc parameters: %d != 4 or 5 or 6",  \
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "sssl|bl", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &increment, &return_errno, \
		&expires) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_inc parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"increment=%ld, expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len, increment, expires);
	*/

	value_len = sizeof(szValue);
	if ((result=fdht_inc_ex(pGroupArray, bKeepAlive, &key_info, \
			expires, increment, szValue, &value_len)) != 0)
	{
		if (return_errno)
		{
			RETURN_LONG(result);
		}
		else
		{
			RETURN_BOOL(false);
		}
	}

	RETURN_STRINGL(szValue, value_len, 1);
}

/*
string/int/boolean fastdht_inc(string namespace, string object_id, string key, 
		int increment [, bool return_errno, int expires])
return string value for success, int value (errno) for error
*/
ZEND_FUNCTION(fastdht_inc)
{
	php_fdht_inc_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

static void php_fdht_delete_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc != 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_delete parameters: %d != 3", 
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	if (zend_parse_parameters(argc TSRMLS_CC, "sss", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_delete parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d)", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len);
	*/

	RETURN_LONG(fdht_delete_ex(pGroupArray, bKeepAlive, &key_info));
}

/*
int fastdht_delete(string namespace, string object_id, string key)
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_delete)
{
	php_fdht_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

static void php_fdht_close(php_fdht_t *i_obj TSRMLS_DC)
{
	if (i_obj->pGroupArray == NULL)
	{
		return;
	}

	if (i_obj->pGroupArray != i_obj->pConfigInfo->pGroupArray)
	{
		fdht_disconnect_all_servers(i_obj->pGroupArray);
	}
	else if (!i_obj->pConfigInfo->keep_alive)
	{
		fdht_disconnect_all_servers(i_obj->pGroupArray);
	}
}

static void php_fdht_stat_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	bool return_errno;
	long server_index;
	char buff[2048];
	int result;
	char *rows[FDHT_STAT_MAX_ROWS];
	int row_count;
	char *pKey;
	char *pValue;
	int key_len;
	int i;

	argc = ZEND_NUM_ARGS();
	if (argc != 1 && argc != 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_stat parameters count: %d != 1 or 2", 
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	if (zend_parse_parameters(argc TSRMLS_CC, "l|b", \
			&server_index, &return_errno) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_stat parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	result = fdht_stat_ex(pGroupArray, bKeepAlive, server_index, 
				buff, sizeof(buff));
	if (result != 0)
	{
		if (return_errno)
		{
			RETURN_LONG(result);
		}
		else
		{
			RETURN_BOOL(false);
		}
	}

	array_init(return_value);
	row_count = splitEx(buff, '\n', rows, FDHT_STAT_MAX_ROWS);
	for (i=0; i<row_count; i++)
	{
		pKey = rows[i];
		pValue = strchr(rows[i], '=');
		if (pValue == NULL)
		{
			continue;
		}
		*pValue = '\0';
		key_len = pValue - pKey;
		pValue++; //skip =

		add_assoc_stringl_ex(return_value, pKey, key_len + 1, \
				pValue, strlen(pValue), 1);
	}
}

static void php_fdht_stat_all_impl(INTERNAL_FUNCTION_PARAMETERS, \
		GroupArray *pGroupArray, bool bKeepAlive)
{
	int argc;
	bool return_errno;
	zval *server_info_array;
	int server_index;
	char buff[2048];
	char server_key[32];
	int result;
	char *rows[FDHT_STAT_MAX_ROWS];
	int row_count;
	char *pKey;
	char *pValue;
	int server_key_len;
	int key_len;
	int i;

	argc = ZEND_NUM_ARGS();
	if (argc != 0 && argc != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_stat_all parameters count: %d != 0 or 1", 
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	return_errno = false;
	if (zend_parse_parameters(argc TSRMLS_CC, "|b", \
			&return_errno) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_stat_all parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	array_init(return_value);
	for (server_index=0; server_index<pGroupArray->server_count; 
		server_index++)
	{
		server_key_len = sprintf(server_key, "%s:%d", 
			pGroupArray->servers[server_index].ip_addr, 
			pGroupArray->servers[server_index].port);
		result = fdht_stat_ex(pGroupArray, bKeepAlive, server_index, 
				buff, sizeof(buff));
		if (result != 0)
		{
			if (return_errno)
			{
				add_assoc_long_ex(return_value, server_key, \
					server_key_len + 1, result);
			}
			else
			{
				add_assoc_bool_ex(return_value, server_key, \
					server_key_len + 1, false);
			}

			continue;
		}

		ALLOC_INIT_ZVAL(server_info_array);
		array_init(server_info_array);

		add_assoc_zval_ex(return_value, server_key, 
			server_key_len + 1, server_info_array);

		row_count = splitEx(buff, '\n', rows, FDHT_STAT_MAX_ROWS);
		for (i=0; i<row_count; i++)
		{
			pKey = rows[i];
			pValue = strchr(rows[i], '=');
			if (pValue == NULL)
			{
				continue;
			}
			*pValue = '\0';
			key_len = pValue - pKey;
			pValue++; //skip =

			add_assoc_stringl_ex(server_info_array, pKey, key_len + 1, \
					pValue, strlen(pValue), 1);
		}
	}
}

/* constructor/destructor */
static void php_fdht_destroy(php_fdht_t *i_obj TSRMLS_DC)
{
	php_fdht_close(i_obj);
	if (i_obj->pGroupArray != NULL && i_obj->pGroupArray != \
		i_obj->pConfigInfo->pGroupArray)
	{
		fdht_free_group_array(i_obj->pGroupArray);
		efree(i_obj->pGroupArray);
		i_obj->pGroupArray = NULL;
	}

	efree(i_obj);
}

ZEND_RSRC_DTOR_FUNC(php_fdht_dtor)
{
	if (rsrc->ptr != NULL)
	{
		php_fdht_t *i_obj = (php_fdht_t *)rsrc->ptr;
		php_fdht_destroy(i_obj TSRMLS_CC);
		rsrc->ptr = NULL;
	}
}

/* FastDHT::__construct([int config_index = 0, bool bMultiThread = false])
   Creates a FastDHT object */
static PHP_METHOD(FastDHT, __construct)
{
	long config_index;
	bool bMultiThread;
	zval *object = getThis();
	php_fdht_t *i_obj;

	config_index = 0;
	bMultiThread = false;
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|lb", \
			&config_index, &bMultiThread) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"zend_parse_parameters fail!", __LINE__);
		ZVAL_NULL(object);
		return;
	}

	if (config_index < 0 || config_index >= config_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid config_index: %d < 0 || >= %d", \
			__LINE__, config_index, config_count);
		ZVAL_NULL(object);
		return;
	}

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	i_obj->pConfigInfo = config_list + config_index;
	if (bMultiThread)
	{
		i_obj->pGroupArray = (GroupArray *)emalloc(sizeof(GroupArray));
		if (i_obj->pGroupArray == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail!", __LINE__, \
				sizeof(GroupArray));
			ZVAL_NULL(object);
			return;
		}

		if (fdht_copy_group_array(i_obj->pGroupArray, \
			i_obj->pConfigInfo->pGroupArray) != 0)
		{
			ZVAL_NULL(object);
			return;
		}
	}
	else
	{
		i_obj->pGroupArray = i_obj->pConfigInfo->pGroupArray;
	}
}

/*
string/int/bool FastDHT::get(string namespace, string object_id, string key
		[, return_errno, int expires])
return string value for success, int value (errno) for error
*/
PHP_METHOD(FastDHT, get)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
int FastDHT::set(string namespace, string object_id, string key, 
		string value [, int expires])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, set)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_set_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
string/int FastDHT::inc(string namespace, string object_id, string key, 
		int increment [, int expires])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, inc)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_inc_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
int FastDHT::delete(string namespace, string object_id, string key)
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, delete)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
int FastDHT::batch_get(string namespace, string object_id, array key_list, 
		[, int expires])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, batch_get)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_batch_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
int FastDHT::batch_set(string namespace, string object_id, array key_list, 
		[, int expires])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, batch_set)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_batch_set_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
int FastDHT::batch_delete(string namespace, string object_id, array key_list)
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, batch_delete)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_batch_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
array/int/boolean FastDHT::get_sub_keys(string namespace, string object_id, \
		[, bool return_errno])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, get_sub_keys)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_get_sub_keys_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		i_obj->pGroupArray, true);
}

/*
void FastDHT::close()
*/
PHP_METHOD(FastDHT, close)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_close(i_obj);
}

/*
array/int/boolean FastDHT::stat(int server_index[, bool return_errno])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, stat)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_stat_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

/*
array/int/boolean FastDHT::stat_all([bool return_errno])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDHT, stat_all)
{
	zval *object = getThis();
	php_fdht_t *i_obj;

	i_obj = (php_fdht_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdht_stat_all_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pGroupArray, true);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_get, 0, 0, 3)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, szKey)
	ZEND_ARG_INFO(0, expires)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_set, 0, 0, 4)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, szKey)
	ZEND_ARG_INFO(0, szValue)
	ZEND_ARG_INFO(0, expires)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_inc, 0, 0, 4)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, szKey)
	ZEND_ARG_INFO(0, increment)
	ZEND_ARG_INFO(0, expires)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_delete, 0, 0, 3)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, szKey)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_batch_get, 0, 0, 3)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, key_list)
	ZEND_ARG_INFO(0, expires)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_batch_set, 0, 0, 3)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, key_list)
	ZEND_ARG_INFO(0, expires)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_batch_delete, 0, 0, 3)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, key_list)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_get_sub_keys, 0, 0, 2)
	ZEND_ARG_INFO(0, szNamespace)
	ZEND_ARG_INFO(0, szObjectId)
	ZEND_ARG_INFO(0, return_errno)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_close, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_stat, 0, 0, 2)
	ZEND_ARG_INFO(0, server_index)
	ZEND_ARG_INFO(0, return_errno)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_stat_all, 0, 0, 1)
	ZEND_ARG_INFO(0, return_errno)
ZEND_END_ARG_INFO()

/* {{{ fdht_class_methods */
#define FDHT_ME(name, args) PHP_ME(FastDHT, name, args, ZEND_ACC_PUBLIC)
static zend_function_entry fdht_class_methods[] = {
    FDHT_ME(__construct,        arginfo___construct)
    FDHT_ME(get,                arginfo_get)
    FDHT_ME(set,                arginfo_set)
    FDHT_ME(inc,                arginfo_inc)
    FDHT_ME(delete,             arginfo_delete)
    FDHT_ME(close,              arginfo_close)
    FDHT_ME(batch_get,          arginfo_batch_get)
    FDHT_ME(batch_set,          arginfo_batch_set)
    FDHT_ME(batch_delete,       arginfo_batch_delete)
    FDHT_ME(get_sub_keys,       arginfo_get_sub_keys)
    FDHT_ME(stat,               arginfo_stat)
    FDHT_ME(stat_all,           arginfo_stat_all)
    { NULL, NULL, NULL }
};
#undef FDHT_ME
/* }}} */

static void php_fdht_free_storage(php_fdht_t *i_obj TSRMLS_DC)
{
	zend_object_std_dtor(&i_obj->zo TSRMLS_CC);
	php_fdht_destroy(i_obj TSRMLS_CC);
}

zend_object_value php_fdht_new(zend_class_entry *ce TSRMLS_DC)
{
	zend_object_value retval;
	php_fdht_t *i_obj;

	i_obj = ecalloc(1, sizeof(php_fdht_t));

	zend_object_std_init( &i_obj->zo, ce TSRMLS_CC );
	retval.handle = zend_objects_store_put(i_obj, \
		(zend_objects_store_dtor_t)zend_objects_destroy_object, \
		(zend_objects_free_object_storage_t)php_fdht_free_storage, \
		NULL TSRMLS_CC);
	retval.handlers = zend_get_std_object_handlers();

	return retval;
}

PHP_FASTDHT_API zend_class_entry *php_fdht_get_ce(void)
{
	return fdht_ce;
}

PHP_FASTDHT_API zend_class_entry *php_fdht_get_exception(void)
{
	return fdht_exception_ce;
}

PHP_FASTDHT_API zend_class_entry *php_fdht_get_exception_base(int root TSRMLS_DC)
{
#if HAVE_SPL
	if (!root)
	{
		if (!spl_ce_RuntimeException)
		{
			zend_class_entry **pce;
			zend_class_entry ***ppce;

			ppce = &pce;
			if (zend_hash_find(CG(class_table), "runtimeexception",
			   sizeof("RuntimeException"), (void **) ppce) == SUCCESS)
			{
				spl_ce_RuntimeException = *pce;
				return *pce;
			}
		}
		else
		{
			return spl_ce_RuntimeException;
		}
	}
#endif
#if (PHP_MAJOR_VERSION == 5) && (PHP_MINOR_VERSION < 2)
	return zend_exception_get_default();
#else
	return zend_exception_get_default(TSRMLS_C);
#endif
}

static int load_config_files()
{
	#define ITEM_NAME_CONF_COUNT "fastdht_client.config_count"
	#define ITEM_NAME_CONF_FILE  "fastdht_client.config_file"
	#define ITEM_NAME_BASE_PATH  	 "fastdht_client.base_path"
	#define ITEM_NAME_CONNECT_TIMEOUT "fastdht_client.connect_timeout"
	#define ITEM_NAME_NETWORK_TIMEOUT "fastdht_client.network_timeout"
	#define ITEM_NAME_LOG_LEVEL      "fastdht_client.log_level"
	#define ITEM_NAME_LOG_FILENAME   "fastdht_client.log_filename"
	zval conf_c;
	zval base_path;
	zval connect_timeout;
	zval network_timeout;
	zval log_level;
	zval log_filename;
	zval conf_filename;
	char szItemName[sizeof(ITEM_NAME_CONF_FILE) + 10];
	char szProxyPrompt[64];
	int nItemLen;
	FDHTConfigInfo *pConfigInfo;
	FDHTConfigInfo *pConfigEnd;
	int result;

	if (zend_get_configuration_directive(ITEM_NAME_CONF_COUNT, 
		sizeof(ITEM_NAME_CONF_COUNT), &conf_c) == SUCCESS)
	{
		config_count = atoi(conf_c.value.str.val);
		if (config_count <= 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"fastdht_client.ini, config_count: %d <= 0!\n",\
				__LINE__, config_count);
			return EINVAL;
		}
	}
	else
	{
		 config_count = 1;
	}

	if (zend_get_configuration_directive(ITEM_NAME_BASE_PATH, \
			sizeof(ITEM_NAME_BASE_PATH), &base_path) != SUCCESS)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"fastdht_client.ini must have item " \
			"\"base_path\"!", __LINE__);
		return ENOENT;
	}

	snprintf(g_fdht_base_path, sizeof(g_fdht_base_path), "%s", \
		base_path.value.str.val);
	chopPath(g_fdht_base_path);
	if (!fileExists(g_fdht_base_path))
	{
		logError("\"%s\" can't be accessed, error info: %s", \
			g_fdht_base_path, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}
	if (!isDir(g_fdht_base_path))
	{
		logError("\"%s\" is not a directory!", g_fdht_base_path);
		return ENOTDIR;
	}

	if (zend_get_configuration_directive(ITEM_NAME_CONNECT_TIMEOUT, \
			sizeof(ITEM_NAME_CONNECT_TIMEOUT), \
			&connect_timeout) == SUCCESS)
	{
		g_fdht_connect_timeout = atoi(connect_timeout.value.str.val);
		if (g_fdht_connect_timeout <= 0)
		{
			g_fdht_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
		}
	}
	else
	{
		g_fdht_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
	}

	if (zend_get_configuration_directive(ITEM_NAME_NETWORK_TIMEOUT, \
			sizeof(ITEM_NAME_NETWORK_TIMEOUT), \
			&network_timeout) == SUCCESS)
	{
		g_fdht_network_timeout = atoi(network_timeout.value.str.val);
		if (g_fdht_network_timeout <= 0)
		{
			g_fdht_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}
	}
	else
	{
		g_fdht_network_timeout = DEFAULT_NETWORK_TIMEOUT;
	}

	if (zend_get_configuration_directive(ITEM_NAME_LOG_LEVEL, \
			sizeof(ITEM_NAME_LOG_LEVEL), \
			&log_level) == SUCCESS)
	{
		set_log_level(log_level.value.str.val);
	}

	log_init();

	if (zend_get_configuration_directive(ITEM_NAME_LOG_FILENAME, \
			sizeof(ITEM_NAME_LOG_FILENAME), \
			&log_filename) == SUCCESS)
	{
		if (log_filename.value.str.len > 0)
		{
			log_set_prefix(g_fdht_base_path, log_filename.value.str.val);
		}
	}

	config_list = (FDHTConfigInfo *)malloc(sizeof(FDHTConfigInfo) * \
			config_count);
	if (config_list == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail!\n",\
			__LINE__, (int)sizeof(FDHTConfigInfo) * config_count);
		return errno != 0 ? errno : ENOMEM;
	}

	pConfigEnd = config_list + config_count;
	for (pConfigInfo=config_list; pConfigInfo<pConfigEnd; pConfigInfo++)
	{
		nItemLen = sprintf(szItemName, "%s%d", ITEM_NAME_CONF_FILE, \
				(int)(pConfigInfo - config_list));
		if (zend_get_configuration_directive(szItemName, \
			nItemLen + 1, &conf_filename) != SUCCESS)
		{
			if (pConfigInfo != config_list)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"fastdht_client.ini: get param %s " \
					"fail!\n", __LINE__, szItemName);

				return ENOENT;
			}

			if (zend_get_configuration_directive( \
				ITEM_NAME_CONF_FILE, \
				sizeof(ITEM_NAME_CONF_FILE), \
				&conf_filename) != SUCCESS)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"fastdht_client.ini: get param %s " \
					"fail!\n",__LINE__,ITEM_NAME_CONF_FILE);

				return ENOENT;
			}
		}

		if (pConfigInfo == config_list) //first config file
		{
			pConfigInfo->pGroupArray = &g_group_array;
		}
		else
		{
			pConfigInfo->pGroupArray = (GroupArray *)malloc( \
							sizeof(GroupArray));
			if (pConfigInfo->pGroupArray == NULL)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"malloc %d bytes fail!\n", \
					__LINE__, (int)sizeof(GroupArray));
				return errno != 0 ? errno : ENOMEM;
			}
		}


		if ((result=fdht_load_conf(conf_filename.value.str.val,\
			pConfigInfo->pGroupArray, \
			&pConfigInfo->keep_alive)) != 0)
		{
			return result;
		}

		if (pConfigInfo == config_list) //first config file
		{
			g_keep_alive = pConfigInfo->keep_alive;
		}
	}

	if (g_group_array.use_proxy)
	{
		sprintf(szProxyPrompt, "proxy_addr=%s, proxy_port=%d, ",
				g_group_array.proxy_server.ip_addr, 
				g_group_array.proxy_server.port);
	}
	else
	{
		*szProxyPrompt = '\0';
	}

	logDebug("base_path=%s, connect_timeout=%ds, network_timeout=%ds. " \
		"in the first(default) config file: keep_alive=%d, " \
		"use_proxy=%d, %s" \
		"group_count=%d, server_count=%d", \
		g_fdht_base_path, g_fdht_connect_timeout, \
		g_fdht_network_timeout, g_keep_alive, \
		g_group_array.use_proxy, szProxyPrompt, \
		g_group_array.group_count, g_group_array.server_count);

	return 0;
}

PHP_MINIT_FUNCTION(fastdht_client)
{
	zend_class_entry ce;

	if (load_config_files() != 0)
	{
		return FAILURE;
	}

	le_fdht = zend_register_list_destructors_ex(NULL, php_fdht_dtor, \
			"FastDHT", module_number);

	INIT_CLASS_ENTRY(ce, "FastDHT", fdht_class_methods);
	fdht_ce = zend_register_internal_class(&ce TSRMLS_CC);
	fdht_ce->create_object = php_fdht_new;

	INIT_CLASS_ENTRY(ce, "FastDHTException", NULL);
	fdht_exception_ce = zend_register_internal_class_ex(&ce, \
		php_fdht_get_exception_base(0 TSRMLS_CC), NULL TSRMLS_CC);

	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NEVER", 0, CONST_CS|CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NONE", -1, CONST_CS|CONST_PERSISTENT);

	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(fastdht_client)
{
	FDHTConfigInfo *pConfigInfo;
	FDHTConfigInfo *pConfigEnd;

	if (config_list != NULL)
	{
		pConfigEnd = config_list + config_count;
		for (pConfigInfo=config_list; pConfigInfo<pConfigEnd; \
			pConfigInfo++)
		{
			if (pConfigInfo->keep_alive && \
				pConfigInfo->pGroupArray != NULL)
			{
				fdht_disconnect_all_servers( \
						pConfigInfo->pGroupArray);
			}
		}
	}

	fdht_client_destroy();

	return SUCCESS;
}

PHP_RINIT_FUNCTION(fastdht_client)
{
	return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(fastdht_client)
{
	fprintf(stderr, "request shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}

PHP_MINFO_FUNCTION(fastdht_client)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "fastdht_client support", "enabled");
	php_info_print_table_end();

}

/*
array/int/boolean fastdht_stat(int server_index[, bool return_errno])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_stat)
{
	php_fdht_stat_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

/*
array/int/boolean fastdht_stat_all([bool return_errno])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_stat_all)
{
	php_fdht_stat_all_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_group_array, g_keep_alive);
}

