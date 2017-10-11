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


public class Cassandra_Test 
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
	
	public void create_keyspace(String spacename, String VarA){
		StringBuilder createspace = new StringBuilder();
		createspace.append("create keyspace if not exists ")
		.append(spacename)
		.append(" with replication={'class':'"+ VarA + "', 'replication_factor' : 1};");
		String create = createspace.toString();
		session.execute(create);
		//System.out.println(create);
	}
	
	public void create_keycolmns(ArrayList<String> list, String space_name, String table_name) {
		int i;
		StringBuilder createtable = new StringBuilder();
		createtable.append("create table if not exists " + space_name + "." + table_name + "(");
		for (i = 0; i < list.size() - 1; i ++) {
			createtable.append(list.get(i) + ",");
		}
		createtable.append(list.get(i) + ");");
		String tab = createtable.toString();
		System.out.println("---------" + tab);
		session.execute(tab);
	}
	
	public void insertdata(String space_name, String table_name, String columns, String values){
		StringBuilder insert = new StringBuilder();
		insert.append("insert into ")
		.append(space_name + "." + table_name)
		.append("(" + columns + ") ")
		.append("values(" + values + ");");
		String ins = insert.toString();
		System.out.println(ins);
		session.execute(ins);
	}
	
	public void query(String queryCQL) {
		System.out.println(queryCQL);
		ResultSet rs = session.execute(queryCQL);
		List<Row> dataList = rs.all();
		
		for (Row row : dataList) {
			System.out.println("=>name: " + row.getString("name"));
			System.out.println("=>age : " + row.getInt("age"));
		}
	}
	
	public void update(String space_name, String table_name, String target, String condition) {
		StringBuilder update = new StringBuilder();
		update.append("update " + space_name + "." + table_name)
		.append(" set " + target)
		.append(" where " + condition + ";");
		String up = update.toString();
		System.out.println(up);
		session.execute(up);
	}
	
	public void delete(String space_name, String table_name, String condition) {
		StringBuilder delete = new StringBuilder();
		delete.append("delete from ")
		.append(space_name + "." + table_name)
		.append(" where " + condition);
		String de = delete.toString();
		System.out.println(de);
		session.execute(de);
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
	
	public static void main( String[] args )
    {
		Cassandra_Test client = new Cassandra_Test();
		client.connect("localhost");
		String space_name = "testkeyspace1";
		String table_name = "student";
		client.create_keyspace(space_name, "SimpleStrategy");
		ArrayList<String> list = new ArrayList<String>();
		list.add("name varchar primary key");
		list.add("age int");
		client.create_keycolmns(list, space_name, table_name);
		client.insertdata(space_name, table_name, "name,age", "'john',30");
		client.insertdata(space_name, table_name, "name,age", "'lin',20");
		client.query("select * from testkeyspace1.student;");
		client.update(space_name, table_name, "age=23", "name='john'");
		client.query("select * from testkeyspace1.student;");
		client.delete(space_name, table_name, "name='lin'");
		client.query("select * from testkeyspace1.student;");
		client.close();
    	} 

}
    

