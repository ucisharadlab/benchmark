package edu.uci.ics.tippers.wrapper.griddb.translator;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by peeyush on 30/6/17.
 */
public class Converter {

    private static class JSONStringDeserializer<T extends JSONObject> implements JsonDeserializer<T> {
        public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            try {
                if (json.getAsString().isEmpty())
                    return null;
                return (T)(((Class)typeOfT).getConstructor(String.class).newInstance(
                        json.getAsString()));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class JSONSToJSONDeserializer<T extends JSONObject> implements JsonDeserializer<T> {
        public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            try {
                if (json.toString().isEmpty())
                    return null;
                return (T)(((Class)typeOfT).getConstructor(String.class).newInstance(
                        json.toString()));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class TimeStampDeSerializer implements JsonDeserializer {
        public Calendar deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            try {
                Date date = new Date(json.getAsString());
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                return cal;
            } catch (Exception e) {
                ;
            }
            return null;
        }
    }

    public static <T extends JSONObject> JSONStringDeserializer getJSONDeserializer() {
        return new JSONStringDeserializer<T>();
    }

    public static <T extends JSONObject> JSONSToJSONDeserializer getJSONToJSONDeserializer() {
        return new JSONSToJSONDeserializer<T>();
    }

    public static  TimeStampDeSerializer getCalendarDeserializer() {
        return new TimeStampDeSerializer();
    }
}
