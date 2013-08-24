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
