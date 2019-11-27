package edu.uci.ics.tippers.data.sqlserver.mappings;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.sqlserver.SQLServerBaseDataMapping;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class SQLServerDataMapping3 extends SQLServerBaseDataMapping {

    private static final Logger LOGGER = Logger.getLogger(SQLServerDataMapping3.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private int BUCKET_SIZE = 2800;

    private JSONParser parser = new JSONParser();

    public SQLServerDataMapping3(Connection connection, String dataDir) {
        super(connection, dataDir);
    }

    public void addAll() throws BenchmarkException{
        try {
            connection.setAutoCommit(false);
            addLineItemData();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void insertPerformance() {
    }

    public void addLineItemData() throws BenchmarkException {

        PreparedStatement stmt;
        String insert;
        String line;
	int count;
	int bucket;
	BufferedReader reader; 
        try {
	    
	    /*
            // Adding Quantity
            insert ="INSERT INTO LINEITEM_QUANTITY " + "(ID, L_QUANTITY, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Quantity_2mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket = 1; 
	    line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line==null) break;
                stmt.setInt(1, count);
                stmt.setDouble(2, Float.parseFloat(line));
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }

            // Adding ORDERKEY
            insert = "INSERT INTO LINEITEM_ORDER_KEY " + "(ID, L_ORDERKEY, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Order_Key_2mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket= 1;    
            line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line == null) break;
                stmt.setInt(1, count);
                stmt.setInt(2, Integer.parseInt(line));
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }

            // Adding LINESTATUS
            insert = "INSERT INTO LINEITEM_LINESTATUS " +
                    "(ID, L_LINESTATUS, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Line_Status_2mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket = 1;    
            line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line == null) break;
                stmt.setInt(1, count);
                stmt.setString(2, line);
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }*/

	    BUCKET_SIZE = 6325;
            // Adding Quantity
            insert ="INSERT INTO LINEITEM_QUANTITY_BIG " + "(ID, L_QUANTITY, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Quantity_10mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket = 1; 
	    line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line==null) break;
                stmt.setInt(1, count);
                stmt.setDouble(2, Float.parseFloat(line));
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }

            // Adding ORDERKEY
            insert = "INSERT INTO LINEITEM_ORDER_KEY_BIG " + "(ID, L_ORDERKEY, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Order_Key_10mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket= 1;    
            line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line == null) break;
                stmt.setInt(1, count);
                stmt.setInt(2, Integer.parseInt(line));
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }

            // Adding LINESTATUS
            insert = "INSERT INTO LINEITEM_LINESTATUS_BIG " +
                    "(ID, L_LINESTATUS, BUCKET) VALUES (?, ?, ?)";
            reader = new BufferedReader(new FileReader("/home/sgx_admin/LineItem_Line_Status_10mn.csv"));
            stmt = connection.prepareStatement(insert);
            count = 1;
	    bucket = 1;    
            line = reader.readLine();
            while(line!=null){
                line = reader.readLine();
		if (line == null) break;
                stmt.setInt(1, count);
                stmt.setString(2, line);
		stmt.setInt(3, bucket);
                stmt.executeUpdate();
                count += 1;
		if (count%BUCKET_SIZE == 0) bucket += 1;
	    }
        } catch (Exception e) {
		e.printStackTrace();
	}

    }

}

