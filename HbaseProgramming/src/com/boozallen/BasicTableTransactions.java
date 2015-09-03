package com.boozallen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BasicTableTransactions {
	
	//Log
	private static final Log LOG = LogFactory.getLog(BasicTableTransactions.class);
	
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
		return Bytes.toBytes(MD5Hash.getMD5AsHex(nameBytes));
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
		insertRecord(admin, table, "Ben", "Howard");
		insertRecord(admin, table, "Major", "Lazer");
		insertRecord(admin, table, "Jane", "Doze");
	}
	
	//scan for records in the table
	public static void scanTable(Admin admin, Table table){
		//scan to find Ben Howard
		Scan s = new Scan(hashName("Ben","Howard"),hashName("Ben","Howard"));
		System.out.println(hashName("Ben","Howard"));
		System.out.println(hashName("Ben","Howard"));
		System.out.println(hashName("Ben","Howard"));
		System.out.println(hashName("Ben","Howard"));
		//specify a specific column family to scan
		s.addFamily(Bytes.toBytes("customer_data"));
		try {
			LOG.info("Before scanning");
			//scan the table
			ResultScanner results = table.getScanner(s);
			LOG.info("After scanning");
			//loop through the results
			System.out.println(results.next().toString());
			for(Result r =  results.next(); r != null; r = results.next()){
				//print results
				LOG.info("Found it: " + r);
				System.out.println("Found it: " + r);
			}
			results.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
	
		Table table = conn.getTable(tableName);
		//insert records into the table. uncomment the following line if this is your first time running the code
		//insertToTable(admin, table);
		
		//scan the table
		scanTable(admin,table);
		
		//close everything
		admin.close();
		conn.close();
		table.close();
	}

}
