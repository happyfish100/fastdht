<?php

$namespace = 'user';
$object_id = 'test';

$key_value_pair = array();

$key_value_pair['login'] = 'happy_fish100';
$key_value_pair['reg'] = 1235301445;
$key_value_pair['intl'] = 'zh';
$key_value_pair['co'] = 'CN';
$key_value_pair['dz'] = 8;

$fdht = new FastDHT(0);
$result = $fdht->batch_set($namespace, $object_id, $key_value_pair);
if ($result != 0)
{
	var_dump($result);
	error_log("fastdht_batch_set fail partially");
}

$fdht = new FastDHT(0);
$key_value_pair = array('login', 'reg', 'intl', 'co', 'city');
$result = $fdht->batch_get($namespace, $object_id, $key_value_pair);
var_dump($result);

$result = $fdht->batch_delete($namespace, $object_id, $key_value_pair);
if ($result != 0)
{
	var_dump($result);
	error_log("fastdht_batch_delete fail partially");
}
$fdht->close();

$key_value_pair = array();
$key_value_pair['login'] = 'happy_fish100';
$key_value_pair['reg'] = 1235301445;
$key_value_pair['intl'] = 'zh';
$key_value_pair['co'] = 'CN';
$key_value_pair['dz'] = 8;
$result = fastdht_batch_set($namespace, $object_id, $key_value_pair);
if ($result != 0)
{
	var_dump($result);
	error_log("fastdht_batch_set fail partially");
}

$key_value_pair = array('login', 'reg', 'intl', 'co');
$result = fastdht_batch_get($namespace, $object_id, $key_value_pair);
var_dump($result);

$result = fastdht_batch_delete($namespace, $object_id, $key_value_pair);
if ($result != 0)
{
	var_dump($result);
	error_log("fastdht_batch_delete fail partially");
}
?>
