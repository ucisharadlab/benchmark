package edu.uci.ics.tippers.query.sqlserver;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.sqlserver.SQLServerConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class SQLServerQueryManager extends BaseQueryManager {

    private Connection connection;

    // Needed For External Databases
    private Database database = Database.SQLSERVER;

    public SQLServerQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connection = SQLServerConnectionManager.getInstance().getEncryptedConnection();
    }

    // For External Database (CrateDB) Usage
    public SQLServerQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout,
                                 Connection connection) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        this.connection = connection;
        this.database = Database.CRATEDB;
    }

    public Duration runTimedQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
        try {
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(queryNum));
            while (rs.next()) {
                if (writeOutput) {
                    StringBuilder line = new StringBuilder("");
                    for (int i = 1; i <= columnsNumber; i++)
                        line.append(rs.getString(i)).append("\t");
                    writer.writeString(line.toString());
                }
            }
            writer.close();
            rs.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }


    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 3:
                String query = "select * from LINEITEM_QUANTITY where ID=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setInt(1, 1240);
                    return runTimedQuery(stmt, 1);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 3:
                String query = "select * from LINEITEM_ORDER_KEY where L_ORDERKEY =?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setInt(1, 1248);
                    return runTimedQuery(stmt, 2);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 3:
                String query = "select * from LINEITEM_LINESTATUS where ID =?";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setInt(1, 1240);
                    ResultSet rs = stmt.executeQuery();
                    String typeId = null;
                    while (rs.next()) {
                        typeId = rs.getString(1);
                    }
                    rs.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        Instant start = Instant.now();
        List<Integer> ids = new ArrayList<>();

        List<Integer> oids = new ArrayList<>();
        List<Integer> sids = new ArrayList<>();
        List<Integer> qids = new ArrayList<>();
        List<Integer> rids = new ArrayList<>();

        List<Integer> orderKeys = new ArrayList<>();
        List<String> lineStatuses = new ArrayList<>();
        List<Double> quantities = new ArrayList<>();
        List<String> restRow = new ArrayList<>();

        String query = "select * from LINEITEM_ORDER_KEY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            int id;
            while (rs.next()) {
                id = rs.getInt(1);
                oids.add(id);
                orderKeys.add(rs.getInt(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }


        query = "select * from LINEITEM_LINESTATUS where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                sids.add(rs.getInt(1));
                lineStatuses.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        query = "select * from LINEITEM_QUANTITY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                qids.add(rs.getInt(1));
                quantities.add(rs.getDouble(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader("/mnt/bucket_2m_1.csv"));
            String line = reader.readLine();
            while (line != null) {
                String data[] = line.split(",", 2);
                rids.add(Integer.parseInt(data[0]));
                restRow.add(data[1]);
                line = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }


        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(4));
            for (int i = 0; i < oids.size(); i++) {
                if (orderKeys.get(i) == 1) {
                    StringBuilder line = new StringBuilder("");
                    line.append(oids.get(i)).append("\t");
                    line.append(orderKeys.get(i)).append("\t");
                    for (int j = 0; j < sids.size(); j++) {
                        if (sids.get(j) == oids.get(i)) line.append(lineStatuses.get(j)).append("\t");
                    }
                    for (int j = 0; j < qids.size(); j++) {
                        if (qids.get(j) == oids.get(i)) line.append(quantities.get(j)).append("\t");
                    }
                    for (int j = 0; j < rids.size(); j++) {
                        if (rids.get(j) == oids.get(i)) line.append(restRow.get(j)).append("\t");
                    }
                    writer.writeString(line.toString());
                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        Instant start = Instant.now();
        List<Integer> ids = new ArrayList<>();

        List<Integer> oids = new ArrayList<>();
        List<Integer> sids = new ArrayList<>();
        List<Integer> qids = new ArrayList<>();
        List<Integer> rids = new ArrayList<>();

        List<Integer> orderKeys = new ArrayList<>();
        List<String> lineStatuses = new ArrayList<>();
        List<Double> quantities = new ArrayList<>();
        List<String> restRow = new ArrayList<>();

        String query = "select * from LINEITEM_ORDER_KEY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            int id;
            while (rs.next()) {
                id = rs.getInt(1);
                oids.add(id);
                orderKeys.add(rs.getInt(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }


        query = "select * from LINEITEM_LINESTATUS where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                sids.add(rs.getInt(1));
                lineStatuses.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        query = "select * from LINEITEM_QUANTITY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                qids.add(rs.getInt(1));
                quantities.add(rs.getDouble(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader("/mnt/bucket_2m_1.csv"));
            String line = reader.readLine();
            while (line != null) {
                String data[] = line.split(",", 2);
                rids.add(Integer.parseInt(data[0]));
                restRow.add(data[1]);
                line = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(5));
            for (int i = 0; i < oids.size(); i++) {
                if (orderKeys.get(i) == 1) {
                    StringBuilder line = new StringBuilder("");
                    line.append(oids.get(i)).append("\t");
                    line.append(orderKeys.get(i)).append("\t");
                    for (int j = 0; j < qids.size(); j++) {
                        if (qids.get(j) == oids.get(i) && quantities.get(j) == 36.0) {
                            line.append(quantities.get(j)).append("\t");
                            for (int k = 0; k < sids.size(); k++) {
                                if (sids.get(k) == oids.get(i)) line.append(lineStatuses.get(k)).append("\t");
                            }
                            for (int k = 0; k < rids.size(); k++) {
                                if (rids.get(k) == oids.get(i)) line.append(restRow.get(k)).append("\t");
                            }
                            writer.writeString(line.toString());
                        }
                    }

                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        Instant start = Instant.now();
        List<Integer> ids = new ArrayList<>();

        List<Integer> oids = new ArrayList<>();
        List<Integer> sids = new ArrayList<>();
        List<Integer> qids = new ArrayList<>();
        List<Integer> rids = new ArrayList<>();

        List<Integer> orderKeys = new ArrayList<>();
        List<String> lineStatuses = new ArrayList<>();
        List<Double> quantities = new ArrayList<>();
        List<Integer> partKeys = new ArrayList<>();
        List<String> restRow = new ArrayList<>();


        String query = "select * from LINEITEM_ORDER_KEY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            int id;
            while (rs.next()) {
                id = rs.getInt(1);
                oids.add(id);
                orderKeys.add(rs.getInt(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }


        query = "select * from LINEITEM_LINESTATUS where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                sids.add(rs.getInt(1));
                lineStatuses.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        query = "select * from LINEITEM_QUANTITY where BUCKET =?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                qids.add(rs.getInt(1));
                quantities.add(rs.getDouble(2));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader("/mnt/bucket_2m_1.csv"));
            String line = reader.readLine();
            while (line != null) {
                String data[] = line.split(",", 3);
                rids.add(Integer.parseInt(data[0]));
                partKeys.add(Integer.parseInt(data[1]));
                restRow.add(data[2]);
                line = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
            for (int i = 0; i < oids.size(); i++) {
                if (orderKeys.get(i) == 1) {
                    StringBuilder line = new StringBuilder("");
                    line.append(oids.get(i)).append("\t");
                    line.append(orderKeys.get(i)).append("\t");
                    for (int j = 0; j < qids.size(); j++) {
                        if (qids.get(j) == oids.get(i) && quantities.get(j) == 36.0) {
                            line.append(quantities.get(j)).append("\t");
                            for (int k = 0; k < rids.size(); k++) {
                                if (rids.get(k) == oids.get(i) && partKeys.get(k) == 673091) {
                                    line.append(partKeys.get(k)).append("\t")
                                            .append(restRow.get(k)).append("\t");
                                    for (int l = 0; l < sids.size(); l++) {
                                        if (sids.get(l) == oids.get(i)) {
                                            line.append(lineStatuses.get(l)).append("\t");
                                            writer.writeString(line.toString());
                                        }
                                    }
                                }

                            }

                        }
                    }

                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        Instant start = Instant.now();
     /*	List<Integer> ids = new ArrayList<>();
	
	List<Integer> oids = new ArrayList<>();
	List<Integer> sids = new ArrayList<>();
	List<Integer> qids = new ArrayList<>();
	
	List<Integer> orderKeys = new ArrayList<>();
	List<String> lineStatuses = new ArrayList<>();
	List<Double> quantities = new ArrayList<>();


	       String query = "select * from LINEITEM_ORDER_KEY where BUCKET =?";
               try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setInt(1, 1);
                    ResultSet rs = stmt.executeQuery();
                    int id;
                    while(rs.next()) {
                        id = rs.getInt(1);
                        oids.add(id);
                        orderKeys.add(rs.getInt(2));
                    }
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }


		query = "select * from LINEITEM_QUANTITY where BUCKET =?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setInt(1, 1);
                    ResultSet rs = stmt.executeQuery();
                    while(rs.next()) {
			qids.add(rs.getInt(1));
                        quantities.add(rs.getDouble(2));
                    }
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }

        try {
          RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));
	  for(int i=0; i<oids.size(); i++) {
		if (orderKeys.get(i)==1) {
			StringBuilder line = new StringBuilder("");
			line.append(oids.get(i)).append("\t");
			line.append(orderKeys.get(i)).append("\t");
			for (int j=0; j<qids.size();j++) {
                                if (qids.get(j) == oids.get(i)) {
					line.append(quantities.get(j)).append("\t");
                        
				}
			writer.writeString(line.toString());
		}
	  }
	  writer.close();
        } catch (Exception e) {
                e.printStackTrace();
        }*/


        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        return Duration.ZERO;
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        return Duration.ZERO;
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        return Duration.ZERO;
    }

    @Override
    public Duration runQuery11() throws BenchmarkException {
        return Constants.MAX_DURATION;
    }
}
