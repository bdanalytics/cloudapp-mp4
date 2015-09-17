import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableExistsException;

//import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

//import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

	private static void addRow(Table hTable, String key, String hero, String power, String name, String xp) {

	      // Instantiating Put class
              // Hint: Accepts a row name
		Put p = new Put(Bytes.toBytes(key));

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
          	p.addColumn(Bytes.toBytes("personal"), 		Bytes.toBytes("hero"), 		Bytes.toBytes(hero));
          	p.addColumn(Bytes.toBytes("personal"), 		Bytes.toBytes("power"), 	Bytes.toBytes(power));
          	p.addColumn(Bytes.toBytes("professional"), 	Bytes.toBytes("name"), 		Bytes.toBytes(name));
          	p.addColumn(Bytes.toBytes("professional"), 	Bytes.toBytes("xp"),   		Bytes.toBytes(xp));
	
		try {
			hTable.put(p);
			System.out.println(key + " inserted");
		} catch (IOException ex) {
	            System.err.println(ex.getMessage());
        	    ex.printStackTrace();
	            System.exit(1);
		}
	}

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiate HBaseAdmin class
      try {
	   ConnectionFactory.createConnection(config).getAdmin();
      } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
      }
      Connection conn = ConnectionFactory.createConnection(config);
      Admin admin = conn.getAdmin();
      
      // Instantiating table descriptor class
      TableName tableName = TableName.valueOf("powers");
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      try {
	  admin.createTable(tableDescriptor);
          System.out.println(" Table powers created ");
      } catch (TableExistsException ex) {
          System.err.println("TableExistsException: " + ex.getMessage());
          //ex.printStackTrace();
          //System.exit(1);
          System.out.println(" Table " + tableName + " already exists ");
      } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
      }

      // Instantiating HTable class
      //HTable hTable = new HTable(con, tableName);
      Table hTable = conn.getTable(tableName);
     
      // Repeat these steps as many times as necessary
	addRow(hTable, "row1", "superman", 	"strength", 	"clark", "100");
	addRow(hTable, "row2", "batman", 	"money", 	"bruce", "50");
	addRow(hTable, "row3", "wolverine", 	"healing", 	"logan", "75");

      // Save the table -> not needed ?	
      // Close table -> need to keep table open for scanner

      // Instantiate the Scan class
          Scan scan = new Scan();

          // Scanning the required columns
          scan.addColumn(Bytes.toBytes("personal"), 	Bytes.toBytes("hero"));
          //scan.addColumn(Bytes.toBytes("personal"), 	Bytes.toBytes("power"));
          //scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
          //scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"));

          // Getting the scan result
          try (ResultScanner scanner = hTable.getScanner(scan)) {

              // Reading values from scan result
              	for (Result result = scanner.next(); result != null; result = scanner.next()) {
	              //System.out.println("Found row : " + result);
	              System.out.println(result);
		}	
	
	      // Close the scanner
		scanner.close();
          }
     
   
      // Htable closer
	hTable.close();
   }
}

