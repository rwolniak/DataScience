package com.boozallen;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import com.boozallen.BasicTableTransactions;
 
import com.google.protobuf.ServiceException;

public class RemoteConnectionToHBase {
	
	//initialize logger
	private static final Log LOG = LogFactory.getLog(RemoteConnectionToHBase.class);

	public static void main(String[] args) throws IOException {
		//configure log4j so it can run
		Properties props = new Properties();
		props.load(new FileInputStream("/Users/ryanwolniak/Development/Hadoop/hadoop-2.7.1/share/hadoop/tools/sls/sample-conf/log4j.properties"));
		PropertyConfigurator.configure(props);
		
		
		//configuration
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "hortonworks.hbase.vm");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        //trying to connect
        try {
        	//check to see if HBase is running
			HBaseAdmin.checkHBaseAvailable(config);
			LOG.info("HBase is running");
		} catch (ServiceException | IOException e) {
			//print stack trace if it failed to connect for some reason
			e.printStackTrace();
		}
         
        //check some records in the table
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();
		//name of the table to scan. This IS case sensitive
		TableName tableName = TableName.valueOf("business_data");
		//create table object. assuming table already exists
		Table table = conn.getTable(tableName);
		
		
		//scan table first to see what records are in it
		BasicTableTransactions.scanTable(admin,table,"");
		
		//close everything
		conn.close();
		admin.close();
		table.close();

	}

}
