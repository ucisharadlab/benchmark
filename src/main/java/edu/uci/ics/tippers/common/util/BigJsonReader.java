package edu.uci.ics.tippers.common.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

public class BigJsonReader<T> {

    private Class<?> claaz;
    private JsonReader reader;
    private Gson gson;
    private static String datePattern = "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

    public BigJsonReader(String filePath, Class claaz) {
        this.claaz = claaz;
        try {
            reader = new JsonReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            gson = new GsonBuilder()
                    .setDateFormat(datePattern)
                    .registerTypeAdapter(JSONObject.class, Converter.getJSONDeserializer())
                    .create();
            reader.beginArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Big Json File");
        }
    }

    public T readNext() {
        try {
            if (reader.hasNext()) {
                return gson.fromJson(reader, claaz);
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Big Json File");
        }
    }



}
