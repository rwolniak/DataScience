package com.boozallen;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BasicTableTransactions {
	
	//use Murmur hash as hashing algorithm, as it seems to offer the best mix of speed and distribution
	private static final Hash HASH = MurmurHash.getInstance();
	private static final int SEED = -1;
	
	//initialize logger
	private static final Log LOG = LogFactory.getLog(BasicTableTransactions.class);
	
	/**
	 * Method which, given an admin and table creates a new table with the given table name and
	 * family name. If the table already exists, it does nothing.
	 * @param admin
	 * @param table
	 * @param famName1
	 * @throws IOException
	 */
	public static void createTable(Admin admin, TableName table, String famName1) throws IOException{		
		//check if the table 'customer_data' exists. Create it if it does not
		if(!admin.tableExists(table)){
			HTableDescriptor tableDescriptor = new HTableDescriptor(table);
			tableDescriptor.addFamily(new HColumnDescriptor(famName1));
			admin.createTable(tableDescriptor);
		}
	}
	
	/**
	 * Method which hashes a person's name, using the murmur hash function, to a byte array
	 * @param fname
	 * @param lname
	 * @return
	 */
	public static byte[] hashName(String fname, String lname){
		//convert first and last name to byte array
		byte[] nameBytes = Bytes.add(Bytes.toBytes(fname),Bytes.toBytes(lname));
		//hash the byte array so the HBase table is optimized with an even distribution of keys
		return Bytes.toBytes(HASH.hash(nameBytes,nameBytes.length,SEED));
	}
	
	/**
	 * Method which inserts a single name record (first name and last name) into a table, as well as their salary
	 * @param admin
	 * @param table
	 * @param fname
	 * @param lname
	 * @param salary
	 */
	public static void insertNameRecord(Admin admin, Table table, String fname, String lname, int salary){
		//hash the name of the person so the HBase table is optimized with an even distribution of keys
		byte[] hash = hashName(fname, lname);
		//get the current system time and convert it to a byte array
		byte[] time = Bytes.toBytes(System.currentTimeMillis());
		//concatenate the time to the hash to create the row key
		byte[] rowKeyBytes = Bytes.add(hash,time);
		//put the row key into a put object
		Put put = new Put(rowKeyBytes);
		//add the specified column and value to this put operation
		put.addColumn(Bytes.toBytes("artist_data"), Bytes.toBytes("fname"), Bytes.toBytes(fname));
		put.addColumn(Bytes.toBytes("artist_data"), Bytes.toBytes("lname"), Bytes.toBytes(lname));
		put.addColumn(Bytes.toBytes("artist_data"), Bytes.toBytes("salary"),Bytes.toBytes(salary));
		
		//write the new data to the table
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//insert some records into the table
	public static void insertToTable(Admin admin, Table table){
		insertNameRecord(admin, table, "Taylor", "Swift", 64000000);
		insertNameRecord(admin, table, "Ben", "Howard", 110000);
		insertNameRecord(admin, table, "Major", "Lazer", 352000);
		insertNameRecord(admin, table, "Jane", "Doze", 50000);
	}
	
	/**
	 * Scans for records in a table given a key, or portion of a key
	 * @param admin
	 * @param table
	 * @param key
	 */
	public static void scanTable(Admin admin, Table table, String key){
		//scan to find all records
		Scan s = new Scan(key.getBytes(),key.getBytes());
		//specify a specific column family to scan
		s.addFamily(Bytes.toBytes("artist_data"));
		try {
			ResultScanner results = table.getScanner(s);

			//loop through the results
			for(Result r =  results.next(); r != null; r = results.next()){
				//print first and last name of record
				LOG.info("Found it: " + Bytes.toString(r.getValue(Bytes.toBytes("artist_data"), Bytes.toBytes("fname"))) + 
						" " + Bytes.toString(r.getValue(Bytes.toBytes("artist_data"), Bytes.toBytes("lname"))));
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
		createTable(admin, tableName, "artist_data");
	
		Table table = conn.getTable(tableName);
		//insert records into the table. uncomment the following line if this is your first time running the code
		//insertToTable(admin, table);
		
		//close everything
		admin.close();
		conn.close();
		table.close();
	}

}
