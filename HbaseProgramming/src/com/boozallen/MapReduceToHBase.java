package com.boozallen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

import com.boozallen.BasicTableTransactions;
 
import com.google.protobuf.ServiceException;

public class MapReduceToHBase {

	public static void main(String[] args) throws IOException {
		System.out.println("Trying to connect...");
		 
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "hortonworks.hbase.vm");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
 
        try {
        	//check to see if HBase is running
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (ServiceException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("HBase is running!");
         
        //check some records in the table
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();
		//name of the table to scan
		TableName tableName = TableName.valueOf("business_data");
		//create table oject. assuming table already exists
		Table table = conn.getTable(tableName);
		
		BasicTableTransactions.scanTable(admin,table);

	}

}
