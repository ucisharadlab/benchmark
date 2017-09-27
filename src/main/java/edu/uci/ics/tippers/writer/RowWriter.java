package edu.uci.ics.tippers.writer;

import com.google.gson.stream.JsonWriter;
import edu.uci.ics.tippers.common.Database;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.ParameterizedType;

public class RowWriter<T> {

    protected Class<?> type;
    protected Writer stringWriter;
    protected JsonWriter jsonWriter;

    public RowWriter(String outputDir, Database database, int mapping, String fileName) throws IOException {
          // TODO: Fix Getting Generic Types by Using SuperClass
//        ParameterizedType t = (ParameterizedType) getClass().getGenericSuperclass();
//        type = (Class<?>) t.getActualTypeArguments()[0];
        type = String.class;

        File dir = new File(new File(outputDir, database.getName()), String.valueOf(mapping));
        dir.mkdirs();

        String path = new File(dir, fileName).getPath();

        if (type == JSONObject.class) {
            jsonWriter = new JsonWriter(new FileWriter(path));
        } else if (type == String.class) {
            stringWriter = new BufferedWriter(new FileWriter(path));
        }

    }

    public void writeToFile(T row) throws IOException {
        if (type == JSONObject.class) {
            writeJson((JSONObject)row);
        } else if (type == String.class) {
            writeString((String)row);
        }
    }

    public void writeJson(JSONObject row) {

    }

    public void writeString(String row) throws IOException {
        stringWriter.write(row);
        stringWriter.write("\n");
    }

    public void close() throws IOException {
        if (type == JSONObject.class) {
            jsonWriter.close();
        } else if (type == String.class) {
            stringWriter.close();
        }
    }


}
