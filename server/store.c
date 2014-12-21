
#include "store.h"
#include "global.h"
#include "db_op.h"
#include "mpool_op.h"

func_destroy_instance g_func_destroy_instance = NULL;
func_destroy g_func_destroy = NULL;
func_memp_trickle g_func_memp_trickle = NULL;
func_get g_func_get = NULL;
func_set g_func_set = NULL;
func_partial_set g_func_partial_set = NULL;
func_delete g_func_delete = NULL;
func_inc g_func_inc = NULL;
func_inc_ex g_func_inc_ex = NULL;
func_clear_expired_keys g_func_clear_expired_keys = NULL;

void store_init()
{
	if (g_store_type == FDHT_STORE_TYPE_BDB)
	{
		g_func_destroy_instance = db_destroy_instance;
		g_func_destroy = db_destroy;
		g_func_memp_trickle = db_memp_trickle;
		g_func_get = db_get;
		g_func_set = db_set;
		g_func_partial_set = db_partial_set;
		g_func_delete = db_delete;
		g_func_inc = db_inc;
		g_func_inc_ex = db_inc_ex;
		g_func_clear_expired_keys = db_clear_expired_keys;
	}
	else
	{
		g_func_destroy_instance = mp_destroy_instance;
		g_func_destroy = mp_destroy;
		g_func_memp_trickle = mp_memp_trickle;
		g_func_get = mp_get;
		g_func_set = mp_set;
		g_func_partial_set = mp_partial_set;
		g_func_delete = mp_delete;
		g_func_inc = mp_inc;
		g_func_inc_ex = mp_inc_ex;
		g_func_clear_expired_keys = mp_clear_expired_keys;
	}
}

