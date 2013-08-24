package com.redgate.hadoop.hive.azuretables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;
import com.microsoft.windowsazure.services.table.client.TableConstants;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableQuery.QueryComparisons;

/**
 * A Record Reader to read Azure Table entities as Records in a Hive Table
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * 
 */
public class AzureTablesReader implements RecordReader<Text, MapWritable> {

	private long pos;
	private Iterator<DynamicTableEntity> results;

	/**
	 * Create a new Azure Table Reader
	 * 
	 * @param storageConnectionString
	 *            An Azure Table connection string, usually built from the
	 *            InputFormat
	 * @param table
	 *            The name of the Azure table, specified in the Hive table
	 *            definition
	 * @param split
	 */
	public AzureTablesReader(String storageConnectionString, String table,
			InputSplit split) {

		AzureTablesSplit partitionSplit = (AzureTablesSplit) split;

		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);

			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			String partitionFilter = TableQuery.generateFilterCondition(
					TableConstants.PARTITION_KEY, QueryComparisons.EQUAL,
					partitionSplit.getPartitionKey());
			TableQuery<DynamicTableEntity> partitionQuery = TableQuery.from(
					table, DynamicTableEntity.class).where(partitionFilter);

			results = tableClient.execute(partitionQuery).iterator();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * No need to close anything, since the MS SDK handles that, but good idea
	 * to remove the resultset
	 */
	public void close() throws IOException {
		results = null;
	}

	/**
	 * Grabs the next result and process the DynamicTableEntity into a Hive
	 * friendly MapWriteable
	 * 
	 * @param key
	 *            The RowID for the entity. Not that this is not really an Azure
	 *            key, since the partition is implicit in the key
	 * @param value
	 *            A MapWriteable which will be populated with values from the
	 *            DynamicTableEntity returned by the Azure query.
	 */
	public boolean next(Text key, MapWritable value) throws IOException {
		if (!results.hasNext())
			return false;
		DynamicTableEntity entity = results.next();
		key.set(entity.getRowKey());
		for (Entry<String, EntityProperty> entry : entity.getProperties()
				.entrySet()) {
			EntityProperty property = entry.getValue();
			String field = entry.getKey();
			value.put(new Text(field), new EntityPropertyWriteable(property));
		}
		return true;
	}

	/**
	 * This generates a GUID that can be used as a key
	 */
	public Text createKey() {
		UUID uid = UUID.randomUUID();
		return new Text(uid.toString());
	}

	public MapWritable createValue() {
		return new MapWritable();
	}

	public long getPos() throws IOException {
		return this.pos;
	}

	/**
	 * Progress is hard to calculate for this since we do not have a record
	 * count ahead of time, and don't want to have to run an expensive count
	 * query to find it, so this will just return 0
	 */
	public float getProgress() throws IOException {
		return 0;
	}

}
