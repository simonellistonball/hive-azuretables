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

	/**
	 * The current implementation ignores the number of splits requested, and
	 * just produces one split per partition key. This could be improved to
	 * allow multiple partition keys per split for greater efficiency at the
	 * hadoop end. This however depends on being able to determine the size of a
	 * partition key.
	 * 
	 * TODO - batch partition keys into a sensible number of splits.
	 * 
	 * @param conf
	 * @param partitionKeys
	 * @param numSplits
	 * @return
	 */
	public static InputSplit[] getSplits(JobConf conf, String[] partitionKeys,
			int numSplits) {
		int total = partitionKeys.length;
		AzureTablesSplit[] splits = new AzureTablesSplit[total];
		for (int i = 0; i < total; i++) {
			if ((i + 1) == total) {
				splits[i] = new AzureTablesSplit(partitionKeys[i]);
				splits[i].setLastSplit(true);
			} else {
				splits[i] = new AzureTablesSplit(partitionKeys[i]);
			}
		}
		return splits;
	}

	public void readFields(DataInput input) throws IOException {
		partitionKey = input.readUTF();
	}

	public void write(DataOutput output) throws IOException {
		output.writeUTF(partitionKey);
	}

	public long getLength() {
		return 0;
	}

	public String[] getLocations() throws IOException {
		return new String[] { partitionKey };
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
