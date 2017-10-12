package edu.uci.ics.tippers.query.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;



public class Cassandra_Insertdata 
{
	private Cluster cluster;
	private Session session;
	
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).withPort(9042).build();
		//get session
		Metadata metadata = cluster.getMetadata();
        for(Host host : metadata.getAllHosts()) {
        		System.out.println("==>" + host.getAddress());
        };
        
        System.out.println("===============");
        
        for(KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces()) {
        	System.out.println("==>" + keyspaceMetadata.getName());
        }
		session = cluster.connect();
	}
	
	
	
	
	// flag is to prevent insert same data into one table multiple times
	// flag = 1 to insert data 
	public void insertdata(String space_name, String table_name, String cql, int flag){
		if(flag == 1) {
			StringBuilder insert = new StringBuilder();
			insert.append("insert into ")
			.append(space_name + "." + table_name)
			.append(" JSON ")
			.append("'" + cql + "'");
			String ins = insert.toString();
			//System.out.println(ins);
			session.execute(ins);
		}
	}
	
	/*public void query(String queryCQL) {
		System.out.println(queryCQL);
		ResultSet rs = session.execute(queryCQL);
		List<Row> dataList = rs.all();
		
		for (Row row : dataList) {
			System.out.println("=>name: " + row.getString("name"));
			System.out.println("=>age : " + row.getInt("age"));
		}
	}*/
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	
	
	public static void main( String[] args )
    {
		Cassandra_Insertdata client = new Cassandra_Insertdata();
		client.connect("localhost");
		String space_name = "tippers";
		String table_name;
		String filename="/Users/ihe/eclipse-workspace/cassandra/src/main/java/com/ymlin/cassandra/observation_test.json";
		String cql;
		int flag;
		
		flag=1;
		table_name = "observation_1";
		/*cql = "{\n" + 
				"    \"id\": \"ISG\",\n" + 
				"    \"name\": \"ISG\",\n" + 
				"    \"description\": \"Information Systems Group\"\n" + 
				"  }";*/
		StringBuilder result = new StringBuilder();;
		File file = new File(filename);
        Reader reader = null;
        try {
            //System.out.println("以字符为单位读取文件内容，一次读一个字节：");
            // 一次读一个字符
            reader = new InputStreamReader(new FileInputStream(file));
            int tempchar;
            int count = 0;
            while ((tempchar = reader.read()) != -1) {
                // 对于windows下，\r\n这两个字符在一起时，表示一个换行。
                // 但如果这两个字符分开显示时，会换两次行。
                // 因此，屏蔽掉\r，或者屏蔽\n。否则，将会多出很多空行。
                if (((char) tempchar) != '\r') {
                	if((char) tempchar == '{') {
                		count ++; 
                	}
                	if ((char) tempchar == '[' || (char) tempchar == ']') continue; 
                    //System.out.print((char) tempchar);
                	result.append((char) tempchar);
                	if((char) tempchar == '}') 
                	{
                		count -- ;
                		if(count == 0) {
                    		String res = result.toString();
                    		//client.insertdata(space_name, table_name, res, 1);
                    		System.out.println("______\n" + res);
                    		result.delete( 0, result.length() );
                    		reader.read();
                    	}
                	}
                	
                	
                		
                	
                	//reader.read();
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //String res = result.toString();
        //System.out.println(res);
        
		//client.insertdata(space_name, table_name, res, 1);
		
		client.close();
    	} 

}
    

