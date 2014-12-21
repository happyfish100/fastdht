<?php

$namespace = 'bbs';
$object_id = 'test';
$key = 'KEY123456';
$value = '123456789012345';

$fdht = new FastDHT(0);

if (($result=$fdht->set($namespace, $object_id, $key, $value)) != 0)
{
	error_log("fastdht_set fail, errno: $result");
}
var_dump($fdht->get($namespace, $object_id, $key, false, time() + 30));

var_dump($fdht->inc($namespace, $object_id, $key, 100));

echo "sub_keys: \n";
var_dump($fdht->get_sub_keys($namespace, $object_id, true));

echo 'delete: ' . $fdht->delete($namespace, $object_id, $key) . "\n";
echo "\n";
var_dump($fdht->stat_all());

$fdht->close();

if (($result=fastdht_set($namespace, $object_id, $key, $value)) != 0)
{
	error_log("fastdht_set fail, errno: $result");
}

$value = fastdht_get($namespace, $object_id, $key, false, time() + 60);
if (!is_string($value))
{
	error_log("fastdht_get fail, errno: $value");
}
else
{
	echo "value: $value\n";
}
echo "\n";
$stats = fastdht_stat(0);
var_dump($stats);

echo "sub_keys: \n";
var_dump(fastdht_get_sub_keys($namespace, $object_id, true));
?>

