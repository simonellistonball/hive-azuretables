package com.redgate.hadoop.hive.azuretables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * The Splits used for AzureTables are based on the Azure partition key.
 * 
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * 
 */
public class AzureTablesSplit implements InputSplit {

	private boolean lastSplit;
	private String partitionKey;

	public AzureTablesSplit(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	public long getLength() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public String[] getLocations() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public static InputSplit[] getSplits(JobConf conf, String[] partitionKeys,
			int numSplits) {
		long total = partitionKeys.length;
		int _numSplits = (numSplits < 1 || total <= numSplits) ? 1 : numSplits;
		AzureTablesSplit[] splits = new AzureTablesSplit[_numSplits];
		for (int i = 0; i < _numSplits; i++) {
			if ((i + 1) == _numSplits) {
				splits[i] = new AzureTablesSplit(partitionKeys[i]);
				splits[i].setLastSplit(true);
			} else {
				splits[i] = new AzureTablesSplit(partitionKeys[i]);
			}
		}
		return splits;
	}

	public boolean isLastSplit() {
		return lastSplit;
	}

	public void setLastSplit(boolean lastSplit) {
		this.lastSplit = lastSplit;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

}
