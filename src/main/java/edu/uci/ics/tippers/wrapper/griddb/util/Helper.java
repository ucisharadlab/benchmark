package edu.uci.ics.tippers.wrapper.griddb.util;

import com.toshiba.mwcloud.gs.GSException;
import edu.uci.ics.tippers.tql.lang.common.clause.LimitClause;
import edu.uci.ics.tippers.tql.lang.common.statement.Query;
import edu.uci.ics.tippers.tql.lang.sqlpp.clause.SelectBlock;
import edu.uci.ics.tippers.tql.lang.sqlpp.expression.SelectExpression;
import org.json.JSONException;
import org.json.JSONObject;

import javax.ws.rs.core.Response;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

/**
 * Created by primpap on 7/8/15.
 */
public class Helper {

	public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String INFINITE_TIMESTAMP = "2038-12-12 00:00:01";
	
	public static final String TIPPERSWEB_DB_URL = "jdbc:mysql://tippersweb.ics.uci.edu:3306/tipperstest";
	public static final String TIPPERSWEB_DB_USER = "test";
	public static final String TIPPERSWEB_DB_PASSWORD = "test";

	private static java.sql.Connection mySQLConnection;

	public static Calendar timestampStrToCal(String timestamp) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT);
		try {
			cal.setTime(sdf.parse(timestamp));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return cal;
	}

	public static String calToTimeStampStr(Calendar calendar) {
		SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT);
		return sdf.format(calendar.getTime());
	}

	public static java.sql.Connection getDBConnection() {

		if (mySQLConnection == null) {
			// Load the Connector/J driver
			try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				//java.sql.Connection conn = DriverManager.getConnection(Helper.SENSORIA_DB_URL, Helper.SENSORIA_DB_USER,
						//Helper.SENSORIA_DB_PASSWORD);
				java.sql.Connection conn = DriverManager.getConnection(Helper.TIPPERSWEB_DB_URL, Helper.TIPPERSWEB_DB_USER,
						Helper.TIPPERSWEB_DB_PASSWORD);
				return conn;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return mySQLConnection;
	}
	
	public static Response createResponse(int errorCode, String errorMessage){
		return Response.status(errorCode)
				.entity(errorMessage)
				.build();
	}
	
	public static boolean isDateValid(String date) 
	{
		try {
			DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
			df.setLenient(false);
			df.parse(date);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	public static JSONObject stripPath(JSONObject jObject) throws JSONException {
		Iterator<?> keys = jObject.keys();
		JSONObject newJsonObject = new JSONObject();
		while( keys.hasNext() ) {
			String key = (String)keys.next();
			String oldKey = key;
			key = key.split("\\.", 2)[1];
			newJsonObject.put(key, jObject.get(oldKey));
		}
		return jObject;
	}

	public static JSONObject handlePayloadInObservation(JSONObject jObject) throws JSONException {
		Iterator<?> keys = jObject.keys();
		JSONObject newJsonObject = new JSONObject();
		JSONObject payload = new JSONObject();

		while( keys.hasNext() ) {
			String key = (String)keys.next();
			if (key.equals("id") || key.equals("timeStamp") || key.equals("sensor")) {
				newJsonObject.put(key, jObject.get(key));
			} else if (key.equals("typeId") ) {
				try {
					newJsonObject.put("type", GridDBManager.genericGetElement(
							"ObservationType", jObject.getString(key)));
				} catch (GSException e) {
					e.printStackTrace();
				}
			} else {
				payload.put(key, jObject.get(key));
			}

		}
		newJsonObject.put("payload", payload);
		return newJsonObject;
	}

	public static JSONObject handlePayloadInSemanticObservation(JSONObject jObject) throws JSONException {
		Iterator<?> keys = jObject.keys();
		JSONObject newJsonObject = new JSONObject();
		JSONObject payload = new JSONObject();

		while( keys.hasNext() ) {
			String key = (String)keys.next();
			if (key.equals("id") || key.equals("timeStamp") || key.equals("type") ) {
				newJsonObject.put(key, jObject.get(key));
			} else if(key.equals("semanticEntityId")) {
				newJsonObject.put(key, jObject.get(key));
				try {
					newJsonObject.put("semanticEntity", GridDBManager.referencedGetElement(
							"Infrastructure", jObject.getString(key)));
				} catch (GSException e) {
					e.printStackTrace();
				}
			} else if (key.equals("virtualSensorId") ) {
				try {
					newJsonObject.put("virtualSensor", GridDBManager.referencedGetElement(
							"VirtualSensor", jObject.getString(key)));
				} catch (GSException e) {
					e.printStackTrace();
				}
			} else {
				payload.put(key, jObject.get(key));
			}

		}
		newJsonObject.put("payload", payload);
		return newJsonObject;
	}

	public static JSONObject appendAttributes(JSONObject row, JSONObject outerScope) throws JSONException {
		Iterator<?> keys = outerScope.keys();
		if (row == null)
			row =  new JSONObject();
		while( keys.hasNext() ) {
			String key = (String)keys.next();
			row.put(key, outerScope.get(key));
		}
		return row;
	}

	public static LimitClause getLimitClause(Query q) {
		return ((SelectExpression)(q.getBody())).getLimitClause();
	}

	public static LimitClause getLimitClause(SelectExpression selectExpression) {
		return selectExpression.getLimitClause();
	}

	public static SelectBlock getSelectBlock(Query q) {
		return ((SelectExpression)(q.getBody())).getSelectSetOperation().getLeftInput().getSelectBlock();
	}



	public static SelectBlock getSelectBlock(SelectExpression selectExpression) {
		return selectExpression.getSelectSetOperation().getLeftInput().getSelectBlock();
	}

}
