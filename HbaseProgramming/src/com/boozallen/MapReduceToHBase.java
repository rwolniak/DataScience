package com.boozallen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
 
import com.google.protobuf.ServiceException;

public class MapReduceToHBase {

	public static void main(String[] args) {
		System.out.println("Trying to connect...");
		 
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "hortonworks.hbase.vm");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
 
        try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (ServiceException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("HBase is running!");
         
        // Do whatever you want

	}

}
