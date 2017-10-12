package edu.uci.ics.tippers.data.cassandra;

import java.util.ArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class cassandra_schema_map {
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
	
	public void create_keyspace(String spacename, String VarA){
		StringBuilder createspace = new StringBuilder();
		createspace.append("create keyspace if not exists ")
		.append(spacename)
		.append(" with replication={'class':'"+ VarA + "', 'replication_factor' : 1};");
		String create = createspace.toString();
		session.execute(create);
		//System.out.println(create);
	}
	
	//FLAG is used to decide whether to order
	public void create_keycolmns(String primary_key, String order_condition, ArrayList<String> list, String space_name, String table_name, int FLAG) {
		int i;
		StringBuilder createtable = new StringBuilder();
		createtable.append("create table if not exists " + space_name + "." + table_name + "(");
		for (i = 0; i < list.size(); i ++) {
			createtable.append(list.get(i) + ",");
		}
		createtable.append("PRIMARY KEY" + primary_key + ") ");
		if(FLAG == 1) {
			createtable.append("WITH CLUSTERING ORDER BY " + order_condition);
		}
		String tab = createtable.toString();
		System.out.println("---------" + tab);
		session.execute(tab);
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	public static void main( String[] args )
    {
		cassandra_schema_map client = new cassandra_schema_map();
		client.connect("localhost");
		String space_name = "tippers";
		String primary_key;
		String order_condition;
		String table_name;
		ArrayList<String> list = new ArrayList<String>();
		client.create_keyspace(space_name, "SimpleStrategy");
		
		//Add Group
		table_name = "Group";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		primary_key = "(id)";
		order_condition = " ";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Add User
		list.clear();
		table_name = "User";
		list.add("emailId varchar");
		list.add("name varchar");
		list.add("groupIds varchar");
		list.add("id varchar");
		list.add("googleAuthToken varchar");
		primary_key = "(emailId)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Add Location
		list.clear();
		table_name = "Location";
		list.add("id varchar");
		list.add("x double");
		list.add("y double");
		list.add("z double");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		
		//Add Region
		list.clear();
		table_name = "Region";
		list.add("id varchar");
		list.add("name varchar");
		list.add("floor INT");
		list.add("geometry list<varchar>");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Add Region——test
				list.clear();
				table_name = "Region_test";
				list.add("id varchar");
				list.add("name varchar");
				list.add("floor INT");
				list.add("geometry list<varchar>");
				primary_key = "(id)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//InfrastructureType
		list.clear();
		table_name = "InfrastructureType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Infrastructure
		list.clear();
		table_name = "Infrastructure";
		list.add("id varchar");
		list.add("name varchar");
		list.add("type_ varchar");//只存里面的id
		list.add("region varchar");// store region id
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Infrastructure_test
				list.clear();
				table_name = "Infrastructure_test";
				list.add("id varchar");
				list.add("name varchar");
				list.add("type_ varchar");//只存里面的id
				list.add("region varchar");// store region id
				primary_key = "(id)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//PlatformType
		list.clear();
		table_name = "PlatformType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Coverage
				list.clear();
				table_name = "Coverage";
				list.add("radius double");
				list.add("id varchar");
				list.add("entitiesCovered list<varchar>"); 
				primary_key = "(id)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
				
				//Coverage_test
				list.clear();
				table_name = "Coverage_test";
				list.add("radius double");
				list.add("id varchar");
				list.add("entitiesCovered list<varchar>"); 
				primary_key = "(id)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Platform
		list.clear();
		table_name = "Platform";
		list.add("id varchar");
		list.add("name varchar");
		list.add("ownerId varchar");
		list.add("typeId varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//SensorType
		list.clear();
		table_name = "SensorType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		list.add("mobility varchar");
		list.add("captureFunctionality varchar");
		list.add("observationTypeId varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Sensor
		list.clear();
		table_name = "Sensor";
		list.add("id varchar");
		list.add("name varchar");
		list.add("coverage varchar");
		list.add("sensorConfig varchar");
		list.add("sensorType varchar");
		list.add("infrastructure varchar");
		list.add("platform varchar");
		list.add("owner varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Sensor_test
				list.clear();
				table_name = "Sensor_test";
				list.add("id varchar");
				list.add("name varchar");
				list.add("coverage varchar");
				list.add("sensorConfig varchar");
				list.add("sensorType varchar");
				list.add("infrastructure varchar");
				list.add("platform varchar");
				list.add("owner varchar");
				primary_key = "(id)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//ObservationType
		list.clear();
		table_name = "ObservationType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		list.add("payloadschema varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		
		//SemanticObservationType
		list.clear();
		table_name = "SemanticObservationType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		list.add("payloadschema varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//VirtualSensorType
		list.clear();
		table_name = "VirtualSensorType";
		list.add("id varchar");
		list.add("name varchar");
		list.add("description varchar");
		list.add("inputTypeId varchar");
		list.add("semanticObservationTypeId varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//VirtualSensor
		list.clear();
		table_name = "VirtualSensor";
		list.add("id varchar");
		list.add("name varchar");
		list.add("typeId varchar");
		list.add("description varchar");
		list.add("language varchar");
		list.add("projectName varchar");
		primary_key = "(id)";
		order_condition = "";
		client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);

		//Observation & SemanticObservation they are in one single table, mapping 1
				list.clear();
				table_name = "Observation_1";
				list.add("typeId varchar");
				list.add("sensorId varchar");
				list.add("timestamp varchar");
				list.add("payload text");
				primary_key = "((sensorId, typeId), timestamp)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
				
				list.clear();
				table_name = "Observation_test";
				list.add("typeId varchar");
				list.add("sensorId varchar");
				list.add("timestamp varchar");
				list.add("payload varchar");
				primary_key = "((sensorId, typeId), timestamp)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
		
		//Observation & SemanticObservation they are in one single table, mapping 2
				list.clear();
				table_name = "Observation_2";
				list.add("typeId varchar");
				list.add("sensorId varchar");
				list.add("timestamp varchar");
				list.add("payload varchar");
				primary_key = "(sensorId, timestamp)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
				
		/*//Observation & SemanticObservation they are in one single table, mapping 3
				list.clear();
				table_name = "Observation_3";
				list.add("id varchar");
				list.add("sensor varchar");
				list.add("timestamp varchar");
				list.add("payload varchar");
				list.add("type varchar");
				primary_key = "((sensor, id), timestamp)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);
				
		//Observation & SemanticObservation they are in one single table, mapping 4
				list.clear();
				table_name = "Observation_4";
				list.add("id varchar");
				list.add("sensor varchar");
				list.add("timestamp varchar");
				list.add("payload varchar");
				list.add("type varchar");
				primary_key = "((sensor, id), timestamp)";
				order_condition = "";
				client.create_keycolmns(primary_key, order_condition, list, space_name, table_name, 0);*/
		
		
		client.close();
    	} 
}
