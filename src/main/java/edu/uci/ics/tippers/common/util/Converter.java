package edu.uci.ics.tippers.common.util;

import com.google.gson.*;
import org.json.JSONObject;

import java.lang.reflect.Type;

/**
 * Created by peeyush on 1/10/17.
 */
public class Converter {

    private static class JSONStringSerializer<T> implements JsonSerializer<T> {
        // TODO: Incomplete
        public JsonElement serialize(T entity, Type typeOfT, JsonSerializationContext context)
                throws JsonParseException {
            return new JsonObject();
        }
    }

    private static class JSONStringDeserializer implements JsonDeserializer {
        public JSONObject deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            if (json.toString().isEmpty())
                return null;
            return new JSONObject(json.toString());
        }
    }

    public static <T> JSONStringSerializer getJSONSerializer() {
        return new JSONStringSerializer<T>();
    }

    public static JSONStringDeserializer getJSONDeserializer() {
        return new JSONStringDeserializer();
    }

}

