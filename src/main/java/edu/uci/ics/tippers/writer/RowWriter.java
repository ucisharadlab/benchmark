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

    public RowWriter(String outputDir, Database database, String fileName) throws IOException {

        ParameterizedType t = (ParameterizedType) RowWriter.class.getGenericSuperclass();
        type = (Class<?>) t.getActualTypeArguments()[0];

        String path = new File(new File(outputDir, database.getName()), fileName).getPath();

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
