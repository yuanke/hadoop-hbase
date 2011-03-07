package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

public class DataNodeDiskErrorException extends IOException {

	public DataNodeDiskErrorException(String msg) {
		super(msg);
	}
	


}
