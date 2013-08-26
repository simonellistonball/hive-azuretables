package com.redgate.hadoop.hive.azuretables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class AzureTablesRecordReader implements RecordReader<Text, MapWritable> {
	public static final Log LOG = LogFactory
			.getLog(AzureTablesRecordReader.class);

	private static final String SERIALIZED_NULL = NullWritable.get().toString();
	private long pos = 0;
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
	public AzureTablesRecordReader(String storageConnectionString,
			String table, InputSplit split) {

		AzureTablesSplit partitionSplit = (AzureTablesSplit) split;

		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);

			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();
			LOG.info(String.format("Connecting to Windows Azure Account: %s",
					storageAccount));
			String partitionFilter = TableQuery.generateFilterCondition(
					TableConstants.PARTITION_KEY, QueryComparisons.EQUAL,
					partitionSplit.getPartitionKey());
			TableQuery<DynamicTableEntity> partitionQuery = TableQuery.from(
					table, DynamicTableEntity.class).where(partitionFilter);

			results = tableClient.execute(partitionQuery).iterator();
		} catch (InvalidKeyException e) {
			LOG.error(e.getMessage());
		} catch (URISyntaxException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * No need to close anything, since the MS SDK handles that, but good idea
	 * to remove the result set
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

			final EntityProperty property = entry.getValue();
			// Note that azure table entity keys are forced to lower case for
			// matching with hive column names
			final String propertyKey = entry.getKey().toLowerCase();
			final String propertyValue = property.getValueAsString();
			final Writable writableValue = SERIALIZED_NULL
					.equals(propertyValue) ? NullWritable.get() : new Text(
					propertyValue);
			value.put(new Text(propertyKey), writableValue);
		}
		pos++;
		return true;
	}

	/**
	 * This generates a GUID that can be used as a key and converts it to a Text
	 * object
	 */
	public Text createKey() {
		UUID uid = UUID.randomUUID();
		return new Text(uid.toString());
	}

	/**
	 * Creates an empty map to be used for the object
	 */
	public MapWritable createValue() {
		return new MapWritable();
	}

	/**
	 * Returns the current record being processed (zero indexed)
	 */
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
