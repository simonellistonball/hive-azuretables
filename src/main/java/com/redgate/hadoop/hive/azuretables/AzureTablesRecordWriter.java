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
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;
import com.microsoft.windowsazure.services.table.client.TableOperation;

/**
 * This writes Azure Tables entries out from MapWriteables in Hadoop.
 * 
 * TODO - This is a pretty naive implementation in that it only sends the
 * entries one per operation, so there is a lot of overhead. A better way would
 * be to batch them into TablelBatchOperation units and manage the control /
 * buffering here to send up sensible size units (which should probably also be
 * system configurable to allow sensible tradeoffs to account for network
 * latency etc).
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * 
 */
public class AzureTablesRecordWriter implements RecordWriter {
	private String table;
	private String partitionKey;
	private CloudTableClient tableClient;
	private HashMap<String, EntityProperty> properties = new HashMap<String, EntityProperty>();

	public AzureTablesRecordWriter(String storageConnectionString,
			String table, String partitionKey) {
		super();
		this.table = table;
		this.partitionKey = partitionKey;

		try {
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(storageConnectionString);
			// Create the table client.
			tableClient = storageAccount.createCloudTableClient();

		} catch (URISyntaxException e) {

		} catch (InvalidKeyException e) {

		}
	}

	/**
	 * Writes a MapWriteable out to the Azure Table
	 */
	public void write(Writable w) throws IOException {
		MapWritable map = (MapWritable) w;
		properties.clear();
		for (Entry<Writable, Writable> e : map.entrySet()) {
			// TODO - more intelligent type mapping (make everything a string is
			// hardly subtle
			EntityProperty value = new EntityProperty(e.getValue().toString());
			properties.put(e.getKey().toString(), value);
		}
		DynamicTableEntity entity = new DynamicTableEntity(properties);
		entity.setPartitionKey(partitionKey);
		entity.setRowKey(UUID.randomUUID().toString());

		TableOperation op = TableOperation.insert(entity);

		try {
			tableClient.execute(table, op);
		} catch (StorageException e) {
			throw new IOException(e);
		}
	}

	/**
	 * This is a noop since the Microsoft SDK handles the connection
	 */
	public void close(boolean abort) throws IOException {
	}

}
