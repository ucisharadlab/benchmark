package edu.uci.ics.tippers.wrapper.griddb.util;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by peeyush on 2/5/17.
 */
public class GridDBManager {

    private static final Logger LOGGER = Logger.getLogger(GridDBManager.class);


    public GridDBManager() {
    }

    public static JSONObject genericGetElement(String name, String id) throws GSException, JSONException {
        GridStore gridStore = null;
        try {
            gridStore = StoreManager.getInstance().getGridStore();
            Container<String, Row> container = gridStore.getContainer(name);
            ContainerInfo containerInfo;
            LOGGER.info("SELECT * FROM " + name + " WHERE id=" + id + ";");
            Row row = container.get(id);
            if (row == null)
                return new JSONObject();
            containerInfo = row.getSchema();

            int columnCount = containerInfo.getColumnCount();
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < columnCount; i++) {
                ColumnInfo columnInfo = containerInfo.getColumnInfo(i);
                if (columnInfo.getType() == GSType.BOOL_ARRAY
                        || columnInfo.getType() == GSType.DOUBLE_ARRAY
                        || columnInfo.getType() == GSType.FLOAT_ARRAY
                        || columnInfo.getType() == GSType.INTEGER_ARRAY
                        || columnInfo.getType() == GSType.STRING_ARRAY)
                    jsonObject.put(columnInfo.getName(), new JSONArray(Arrays.asList(row.getValue(i))));
                jsonObject.put(columnInfo.getName(), row.getValue(i));
            }
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            gridStore.close();

        }
        return null;
    }

    public static JSONObject referencedGetElement(String name, String id) throws GSException, JSONException {
        GridStore gridStore = null;
        try {
            gridStore = StoreManager.getInstance().getGridStore();

            Container<String, Row> container = gridStore.getContainer(name);
            ContainerInfo containerInfo;
            LOGGER.info("SELECT * FROM " + name + " WHERE id=" + id + ";");
            Row row = container.get(id);
            if (row == null)
                return new JSONObject();
            containerInfo = row.getSchema();
            Map<String, String[]> schemaMap = GridDBCollections.referenced.get(name);

            int columnCount = containerInfo.getColumnCount();
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < columnCount; i++) {
                ColumnInfo columnInfo = containerInfo.getColumnInfo(i);
                if (schemaMap.containsKey(columnInfo.getName())) {
                    String nextCollection = schemaMap.get(columnInfo.getName())[0];
                    String attrName = schemaMap.get(columnInfo.getName())[1];
                    if (GridDBCollections.referenced.containsKey(nextCollection)) {
                        if (columnInfo.getType() == GSType.BOOL_ARRAY
                                || columnInfo.getType() == GSType.DOUBLE_ARRAY
                                || columnInfo.getType() == GSType.FLOAT_ARRAY
                                || columnInfo.getType() == GSType.INTEGER_ARRAY
                                || columnInfo.getType() == GSType.STRING_ARRAY) {
                            String[] ids = row.getStringArray(i);
                            JSONArray jsonArray = new JSONArray();
                            for (int j = 0; j < ids.length; j++) {
                                jsonArray.put(referencedGetElement(nextCollection, ids[j]));
                            }
                            jsonObject.put(attrName, jsonArray);
                        } else
                            jsonObject.put(attrName, referencedGetElement(nextCollection, row.getString(i)));
                    } else {
                        if (columnInfo.getType() == GSType.BOOL_ARRAY
                                || columnInfo.getType() == GSType.DOUBLE_ARRAY
                                || columnInfo.getType() == GSType.FLOAT_ARRAY
                                || columnInfo.getType() == GSType.INTEGER_ARRAY
                                || columnInfo.getType() == GSType.STRING_ARRAY) {
                            String[] ids = row.getStringArray(i);
                            JSONArray jsonArray = new JSONArray();
                            for (int j = 0; j < ids.length; j++) {
                                jsonArray.put(genericGetElement(nextCollection, ids[j]));
                            }
                            jsonObject.put(attrName, jsonArray);
                        } else
                            jsonObject.put(attrName, genericGetElement(nextCollection, row.getString(i)));
                    }
                    continue;
                }

                if (columnInfo.getType() == GSType.BOOL_ARRAY
                        || columnInfo.getType() == GSType.DOUBLE_ARRAY
                        || columnInfo.getType() == GSType.FLOAT_ARRAY
                        || columnInfo.getType() == GSType.INTEGER_ARRAY
                        || columnInfo.getType() == GSType.STRING_ARRAY)
                    jsonObject.put(columnInfo.getName(), new JSONArray(Arrays.asList(row.getValue(i))));
                else
                    jsonObject.put(columnInfo.getName(), row.getValue(i));
            }
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            gridStore.close();
        }
        return null;
    }



    public static JSONArray genericGetData(String name) throws GSException, JSONException {
        GridStore gridStore = null;
        try {
            gridStore = StoreManager.getInstance().getGridStore();
            Container<String, Row> container = gridStore.getContainer(name);
            String query = String.format("SELECT * FROM %s", name);
            LOGGER.info(String.format("SELECT * FROM %s", name));
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();
            ContainerInfo containerInfo;
            Row row;

            JSONArray jsonArray = new JSONArray();

            while (rows.hasNext()) {
                row = rows.next();
                containerInfo = row.getSchema();
                int columnCount = containerInfo.getColumnCount();
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < columnCount; i++) {
                    ColumnInfo columnInfo = containerInfo.getColumnInfo(i);
                    if (columnInfo.getType() == GSType.BOOL_ARRAY
                            || columnInfo.getType() == GSType.DOUBLE_ARRAY
                            || columnInfo.getType() == GSType.FLOAT_ARRAY
                            || columnInfo.getType() == GSType.INTEGER_ARRAY
                            || columnInfo.getType() == GSType.STRING_ARRAY)
                        jsonObject.put(columnInfo.getName(), new JSONArray(Arrays.asList(row.getValue(i))));
                    else
                        jsonObject.put(columnInfo.getName(), row.getValue(i));
                }
                jsonArray.put(jsonObject);
            }
            return jsonArray;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            gridStore.close();
        }
        return null;
    }

    public static JSONArray referencedGetData(String name) throws GSException, JSONException {
        GridStore gridStore =  null;
        try {
            gridStore = StoreManager.getInstance().getGridStore();


            Container<String, Row> container = gridStore.getContainer(name);
            String query = String.format("SELECT * FROM %s", name);
            LOGGER.info(String.format("SELECT * FROM %s", name));
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();
            ContainerInfo containerInfo;
            Row row;
            Map<String, String[]> schemaMap = GridDBCollections.referenced.get(name);

            JSONArray jsonArray = new JSONArray();

            while (rows.hasNext()) {
                row = rows.next();
                containerInfo = row.getSchema();
                int columnCount = containerInfo.getColumnCount();
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < columnCount; i++) {
                    ColumnInfo columnInfo = containerInfo.getColumnInfo(i);

                    if (schemaMap.containsKey(columnInfo.getName())) {
                        String nextCollection = schemaMap.get(columnInfo.getName())[0];
                        String attrName = schemaMap.get(columnInfo.getName())[1];

                        if (GridDBCollections.referenced.containsKey(nextCollection)) {
                            if (columnInfo.getType() == GSType.BOOL_ARRAY
                                    || columnInfo.getType() == GSType.DOUBLE_ARRAY
                                    || columnInfo.getType() == GSType.FLOAT_ARRAY
                                    || columnInfo.getType() == GSType.INTEGER_ARRAY
                                    || columnInfo.getType() == GSType.STRING_ARRAY) {
                                String[] ids = row.getStringArray(i);
                                JSONArray tempJsonArray = new JSONArray();
                                for (int j = 0; j < ids.length; j++) {
                                    tempJsonArray.put(referencedGetElement(nextCollection, ids[j]));
                                }
                                jsonObject.put(attrName, tempJsonArray);
                                continue;
                            } else
                                jsonObject.put(attrName, referencedGetElement(nextCollection, row.getString(i)));
                        } else {
                            if (columnInfo.getType() == GSType.BOOL_ARRAY
                                    || columnInfo.getType() == GSType.DOUBLE_ARRAY
                                    || columnInfo.getType() == GSType.FLOAT_ARRAY
                                    || columnInfo.getType() == GSType.INTEGER_ARRAY
                                    || columnInfo.getType() == GSType.STRING_ARRAY) {
                                String[] ids = row.getStringArray(i);
                                JSONArray tempJsonArray = new JSONArray();
                                for (int j = 0; j < ids.length; j++) {
                                    tempJsonArray.put(genericGetElement(nextCollection, ids[j]));
                                }
                                jsonObject.put(attrName, tempJsonArray);
                            } else
                                jsonObject.put(attrName, genericGetElement(nextCollection, row.getString(i)));
                        }
                        continue;
                    }

                    if (columnInfo.getType() == GSType.BOOL_ARRAY
                            || columnInfo.getType() == GSType.DOUBLE_ARRAY
                            || columnInfo.getType() == GSType.FLOAT_ARRAY
                            || columnInfo.getType() == GSType.INTEGER_ARRAY
                            || columnInfo.getType() == GSType.STRING_ARRAY)
                        jsonObject.put(columnInfo.getName(), new JSONArray(Arrays.asList(row.getValue(i))));
                    jsonObject.put(columnInfo.getName(), row.getValue(i));
                }
                jsonArray.put(jsonObject);
            }
            return jsonArray;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            gridStore.close();
        }

        return null;

    }



    public static JSONArray getContainerData(String name) throws GSException, JSONException {
        if(GridDBCollections.referenced.containsKey(name))
            return referencedGetData(name);
        else
            return genericGetData(name);
    }

}
