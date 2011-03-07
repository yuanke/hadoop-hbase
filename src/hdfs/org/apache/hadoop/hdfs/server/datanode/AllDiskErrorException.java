package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

public class AllDiskErrorException extends IOException {
	public AllDiskErrorException(String msg) {
		super(msg);
	}
}
