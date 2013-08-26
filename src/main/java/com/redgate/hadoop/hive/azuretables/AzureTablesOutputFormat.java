package com.redgate.hadoop.hive.azuretables;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

public class AzureTablesOutputFormat implements
		OutputFormat<Text, MapWritable>, HiveOutputFormat<Text, MapWritable> {

	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		// TODO Auto-generated method stub

	}

	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
			JobConf conf, Path finalOutPath,
			Class<? extends Writable> valueClass, boolean isCompressed,
			Properties tableProperties, Progressable progress)
			throws IOException {
		
		String storageConnectionString = ConfigurationUtil
				.getStorageConnectionString(conf);
		String partitionKey = ConfigurationUtil.outputPartitionKey(conf);
		String table = ConfigurationUtil.table(conf);
		return new AzureTablesRecordWriter(storageConnectionString, table,
				partitionKey);
	}

	public org.apache.hadoop.mapred.RecordWriter<Text, MapWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		throw new RuntimeException(
				"Error: Writing to Azure Tables is only supported for Hive at present");
	}

}
