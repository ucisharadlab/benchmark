package com.ymlin.cassandra;

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
import com.datastax.driver.mapping.annotations.Query;


public class cassandra_query 
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


	
	public void query(String queryCQL) {
		System.out.println(queryCQL);
		ResultSet rs = session.execute(queryCQL);
		List<Row> dataList = rs.all();
		
		for (Row row : dataList) {
			//System.out.println("=>name: " + row.getString("name"));
			//System.out.println("=>age : " + row.getInt("age"));
			System.out.println(row);
		}
	}
	
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	public static void main( String[] args )
    {
		cassandra_query client = new cassandra_query();
		client.connect("localhost");
		//query 1
		//client.query("select name from tippers.sensor where id = '3146_clwa_6122';");
		//query 2
		/*client.query("SELECT id, name " + 
				"FROM tippers.sensor " + 
				"WHERE name='3145_clwa_5099' AND (SOME e IN " + 
				"coverage.entitiesCovered SATISFIES e.id IN location);" + 
				"");*/
		//query 3
		/*client.query("SELECT timestamp, sensorid, payload  " + 
				"FROM tippers.observation_1  " + 
				"WHERE sensorid = 'emeter8' " +
				"AND timestamp >= '2017-07-11 00:25:00' AND " + 
				"timestamp <= '2017-07-11 20:45:00'" + 
				"ALLOW FILTERING;");*/
		
		//query 4
		/*client.query("SELECT timestamp, sensorid, payload " + 
				"FROM tippers.observation_1 " + 
				"WHERE sensorid ='emeter5','emeter6','emeter2') " +
				"AND timestamp >= '2017-07-11 00:25:00' AND" + 
				"timestamp <= '2017-07-11 20:45:00'" + 
				"ALLOW FILTERING;"
				);	*/
		
		//query 5
		client.query("");
		
		client.close();
    	} 

}
    

