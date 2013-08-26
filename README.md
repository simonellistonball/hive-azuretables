hive-azuretables
================

Connector between Hive and Azure Tables.

This allows you to create EXTERNAL Hive tables which are backed by Azure Storage Tables.


Sample Query
============

	ADD JAR hive-azuretables-0.0.1.jar;

	DROP TABLE IF EXISTS test;
	
	CREATE EXTERNAL TABLE test (Test STRING)
	STORED BY  "com.redgate.hadoop.hive.azuretables.AzureTablesStorageHandler"
	TBLPROPERTIES(
		'azuretables.account_name'='<account_name>',
		'azuretables.access_key'='<storage_key>',
		'azuretables.table'='test',
		'azuretables.partition_keys'='A,B',
		'azuretables.column_map'='test'
	);

	select * from test;