package com.redgate.hadoop.hive.azuretables;

import static com.redgate.hadoop.hive.azuretables.ConfigurationUtil.copyAzureTableProperties;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

@SuppressWarnings("deprecation")
public class AzureTablesStorageHandler implements HiveStorageHandler {
	private Configuration conf;

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends InputFormat> getInputFormatClass() {
		return AzureTablesInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return HiveIgnoreKeyTextOutputFormat.class;
	}

	public Class<? extends SerDe> getSerDeClass() {
		return AzureTablesSerDe.class;
	}

	private void copyProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		copyAzureTableProperties(properties, jobProperties);
	}

	// TODO consider adding a metahook for Azure Table management
	public HiveMetaHook getMetaHook() {
		// TODO Auto-generated method stub
		return null;
	}

	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return new DefaultHiveAuthorizationProvider();
	}

	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		copyProperties(tableDesc, jobProperties);
	}

	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		copyProperties(tableDesc, jobProperties);
	}

	@Deprecated
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		copyProperties(tableDesc, jobProperties);
	}

}
