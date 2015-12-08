hive-azuretables
================

Connector between Hive and Azure Tables.

This allows you to create EXTERNAL Hive tables which are backed by Azure Storage Tables.

Building
--------

The project is a simple maven build, so, 

        mvn package 
        
should do the job, and you will find the JAR you need to upload to the cluster in target/

You might have to do the following

	You'll also need to install the jdo2 jar in your maven repository:
	download jdo2-api-2.3-ec.jar to your working directory
	mvn install:install-file -DgroupId=javax.jdo -DartifactId=jdo2-api -Dversion=2.3-ec -Dpackaging=jar -Dfile=jdo2-api-2.3-ec.jar
	The new jdo jar is available from 
	http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar

From https://issues.apache.org/jira/browse/HIVE-4114

Sample Query
------------

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
