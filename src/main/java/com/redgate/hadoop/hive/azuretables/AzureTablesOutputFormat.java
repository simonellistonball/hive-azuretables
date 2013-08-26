/*******************************************************************************
 * Copyright 2013 Simon Elliston Ball
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
