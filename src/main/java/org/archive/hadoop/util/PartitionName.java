package org.archive.hadoop.util;

import org.apache.hadoop.conf.Configuration;

public class PartitionName {

	public static String getPartitionConfigName(int partition) {
		return String.format("Partition.%d.output-name",partition);
	}
	
	public static void setPartitionOutputName(Configuration conf, int partition, String name) {
		String cName = getPartitionConfigName(partition);
		conf.set(cName, name);
	}

	public static String getPartitionOutputName(Configuration conf, 
			int partition) {
		String cName = getPartitionConfigName(partition);
		return conf.get(cName);
	}
}
