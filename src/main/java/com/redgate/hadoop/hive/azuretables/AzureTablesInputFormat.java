package com.redgate.hadoop.hive.azuretables;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A HiveInputFormat implementation to allow Azure Tables to back Hive External
 * tables
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * 
 */
public class AzureTablesInputFormat extends HiveInputFormat<Text, MapWritable> {
	/**
	 * Sets up a RecordReader for an Azure Table, and build a connection string
	 * from the table properties.
	 */
	@Override
	public RecordReader<Text, MapWritable> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {
		String table = conf.get(ConfigurationUtil.TABLE);
		String storageConnectionString = String.format(
				"DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s",
				conf.get(ConfigurationUtil.ACCOUNT_NAME),
				conf.get(ConfigurationUtil.ACCESS_KEY));

		return new AzureTablesReader(storageConnectionString, table, split);
	}

	/**
	 * Determine InputSplits for the Azure Table reader, using the partition
	 * keys given in the table definition as the input split boundaries.
	 * 
	 * @param conf
	 *            The Hadoop job configuration
	 * @param numSplits
	 *            The desired number of splits, which is pretty much ignored at
	 *            the moment. This is not ideal, since there should be a
	 *            secondary mechanism to further split a table partition
	 */
	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits)
			throws IOException {
		// TODO - need an actually safe way of splitting partition keys
		String[] partitionKeys = conf.get(ConfigurationUtil.PARTITION_KEYS)
				.split(",");
		return AzureTablesSplit.getSplits(conf, partitionKeys, numSplits);
	}
}
