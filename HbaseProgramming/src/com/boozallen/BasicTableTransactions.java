package com.boozallen;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;

public class BasicTableTransactions {
	
	//Murmur hash seems to provide the best medium of speed and distribution
	private static final MurmurHash nameHash = new MurmurHash();
	
	//create table if it doesn't already exist
	private static void createTable(Admin admin, TableName table) throws IOException{		
		//check if the table 'customer_data' exists. Create it if it does not
		if(!admin.tableExists(table)){
			HTableDescriptor tableDescriptor = new HTableDescriptor(table);
			tableDescriptor.addFamily(new HColumnDescriptor("customer_data"));
			admin.createTable(tableDescriptor);
		}
	}
	
	//hash the name of a person using the murmur hash function
	private static byte[] hashName(String fname, String lname){
		//convert first and last name to byte array
		byte[] nameBytes = Bytes.add(Bytes.toBytes(fname),Bytes.toBytes(lname));
		//hash the byte array so the HBase table is optimized with an even distribution of keys
		long hash = nameHash.hash(nameBytes);
		//convert the hash to a byte array and return it
		return Bytes.toBytes(hash);
	}
	
	//insert a single record into the HBase table
	private static void insertRecord(Admin admin, Table table, String fname, String lname){
		//hash the name of the person so the HBase table is optimized with an even distribution of keys
		byte[] hash = hashName(fname, lname);
		//get the current system time and convert it to a byte array
		byte[] time = Bytes.toBytes(System.currentTimeMillis());
		//concatenate the time to the hash to create the row key
		byte[] rowKeyBytes = Bytes.add(hash,time);
		//put the row key into a put object
		Put put = new Put(rowKeyBytes);
		//add the specified column and value to this put operation
		put.addColumn(Bytes.toBytes("customer_data"), Bytes.toBytes("fname"), Bytes.toBytes(fname));
		put.addColumn(Bytes.toBytes("customer_data"), Bytes.toBytes("lname"), Bytes.toBytes(lname));
		//write the new data to the table
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//insert some records into the table
	private static void insertToTable(Admin admin, Table table){
		insertRecord(admin, table, "Taylor", "Swift");
	}

	public static void main(String[] args) throws IOException {	
		//set up connection to Hbase. This method is preferred over the deprecated HBaseAdmin method
		Configuration conf = HBaseConfiguration.create();
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		//name of the table to create or update
		TableName tableName = TableName.valueOf("business_data");
		
		//create the table if it doesn't exist
		createTable(admin, tableName);
		
		//insert records into the table
		Table table = conn.getTable(tableName);
		insertToTable(admin, table);
		
		//close everything
		admin.close();
		conn.close();
		table.close();
	}

}
