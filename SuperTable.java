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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
      Configuration con = HBaseConfiguration.create();

      // Instantiate HBaseAdmin class
      try {
	   ConnectionFactory.createConnection(con).getAdmin();
      } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
      }
      Admin admin = ConnectionFactory.createConnection(con).getAdmin();
      
      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("hero"));
      tableDescriptor.addFamily(new HColumnDescriptor("power"));
      tableDescriptor.addFamily(new HColumnDescriptor("name"));
      tableDescriptor.addFamily(new HColumnDescriptor("xp"));

      // Execute the table through admin
      try {
	  admin.createTable(tableDescriptor);
          System.out.println(" Table powers created ");
      } catch (TableExistsException ex) {
          System.err.println("TableExistsException: " + ex.getMessage());
          //ex.printStackTrace();
          //System.exit(1);
          System.out.println(" Table powers already exists ");
      } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
      }

      // Instantiating HTable class
     
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value

      // Save the table
	
      // Close table

      // Instantiate the Scan class
     
      // Scan the required columns

      // Get the scan result

      // Read values from scan result
      // Print scan result
 
      // Close the scanner
   
      // Htable closer
   }
}

