CREATE EXTERNAL TABLE test (TimeStamp TIMESTAMP, addresses STRING)
ROW FORMAT SERDE 'com.redgate.hadoop.azuretables.AzureTableSerde'
STORED AS 
	INPUTFORMAT 'com.redgate.hadoop.azuretables.AzureTableInputFormat'
	OUTPUTFORMAT 'com.redgate.hadoop.azuretables.AzureTableOutputFormat' 
TBLPROPERTIES(
	azuretables.account_name='visualnode2',
	azuretables.access_key='97IHW7PVwtUYagyf7/TAjUkvM94uqtphgsNONomfeanBMvWPLpTzR+NqOBDyqP6Rh9teoE6zRzc31Iww6YwKZw==',
	azuretables.table='addresses',
	azuretables.partition_keys='visualnode'
	)

SELECT * FROM test


-- and another with multiple keys

CREATE EXTERNAL TABLE test (TimeStamp TIMESTAMP, addresses STRING)
ROW FORMAT SERDE 'com.redgate.hadoop.azuretables.AzureTableSerde'
STORED AS 
	INPUTFORMAT 'com.redgate.hadoop.azuretables.AzureTableInputFormat'
	OUTPUTFORMAT 'com.redgate.hadoop.azuretables.AzureTableOutputFormat' 
TBLPROPERTIES(
	azuretables.account_name='visualnode2',
	azuretables.access_key='97IHW7PVwtUYagyf7/TAjUkvM94uqtphgsNONomfeanBMvWPLpTzR+NqOBDyqP6Rh9teoE6zRzc31Iww6YwKZw==',
	azuretables.table='test',
	azuretables.partition_keys='partiton1,partition2'
	)

SELECT * FROM test



CREATE EXTERNAL TABLE test (TimeStamp TIMESTAMP, addresses STRING)
STORED AS 
	INPUTFORMAT "com.redgate.hadoop.hive.azuretables.AzureTablesInputFormat"
	OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" 
TBLPROPERTIES(
	'azuretables.account_name'='visualnode2',
	'azuretables.access_key'='97IHW7PVwtUYagyf7/TAjUkvM94uqtphgsNONomfeanBMvWPLpTzR+NqOBDyqP6Rh9teoE6zRzc31Iww6YwKZw==',
	'azuretables.table'='addresses',
	'azuretables.partition_keys'='visualnode'
	);

SELECT * FROM test


DROP TABLE IF EXISTS test;
CREATE EXTERNAL TABLE test (test STRING)
STORED AS 
	INPUTFORMAT "com.redgate.hadoop.hive.azuretables.AzureTablesInputFormat"
	OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" 
TBLPROPERTIES(
	'azuretables.account_name'='simonellistonball',
	'azuretables.access_key'='mM7YscoqhXAhy7HnS5IdzKrr3A9K/HsNITk9Gf/W5xFyZmYjE/dj34ZYmmobvxXUrTigf3l40plGbh4ZjJrMhg==',
	'azuretables.table'='test',
	'azuretables.partition_keys'='A,B'
);

SELECT * FROM test


add jar 'hive-azuretables-0.0.3.jar'
add jar 'hive-azuretables-0.0.3-SNAPSHOT-jar-with-dependencies.jar'

DROP TABLE IF EXISTS test;
CREATE EXTERNAL TABLE test (test STRING)
STORED BY  "com.redgate.hadoop.hive.azuretables.AzureTablesStorageHandler"
TBLPROPERTIES(
'azuretables.account_name'='simonellistonball',
'azuretables.access_key'='mM7YscoqhXAhy7HnS5IdzKrr3A9K/HsNITk9Gf/W5xFyZmYjE/dj34ZYmmobvxXUrTigf3l40plGbh4ZjJrMhg==',
'azuretables.table'='test',
'azuretables.partition_keys'='A,B',
'azuretables.column_map'='test'	
);

SELECT * FROM test;

