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

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.ImmutableSet;

/**
 * Constants for the table properties in a CREATE statement
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 */
public class ConfigurationUtil {

	public static final String ACCOUNT_NAME = "azuretables.account_name";
	public static final String ACCESS_KEY = "azuretables.access_key";
	public static final String TABLE = "azuretables.table";
	public static final String PARTITION_KEYS = "azuretables.partition_keys";
	public static final String OUTPUT_PARTITION_KEY = "azuretables.partition_key_output";
	public static final String COLUMN_MAPPING_KEY = "azuretables.column_map";

	private static final String PARTITION_SPLIT = ",";
	private static final Set<String> ALL_PROPERTIES = ImmutableSet.of(
			ACCOUNT_NAME, ACCESS_KEY, TABLE, PARTITION_KEYS,
			OUTPUT_PARTITION_KEY);

	public static String getStorageConnectionString(JobConf conf) {
		return String.format(
				"DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s",
				accountName(conf), accessKey(conf));
	}

	public static String accountName(JobConf conf) {
		return conf.get(ACCOUNT_NAME);
	}

	public static String accessKey(JobConf conf) {
		return conf.get(ACCESS_KEY);
	}

	public static String table(JobConf conf) {
		return conf.get(TABLE);
	}

	public static String[] partitionKeys(JobConf conf) {
		// TODO - need an actually safe way of splitting partition keys
		String keys = conf.get(PARTITION_KEYS);
		return keys.split(PARTITION_SPLIT);
	}

	public static String outputPartitionKey(JobConf conf) {
		String string = conf.get(OUTPUT_PARTITION_KEY);
		if (string == null) {
			string = "OUTPUT";
		}
		return string;
	}

	public static void copyAzureTableProperties(Properties properties,
			Map<String, String> jobProperties) {
		for (String key : ALL_PROPERTIES) {
			String value = properties.getProperty(key);
			if (value != null) {
				jobProperties.put(key, value);
			}
		}
	}

}
