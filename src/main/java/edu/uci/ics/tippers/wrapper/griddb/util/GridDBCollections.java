package edu.uci.ics.tippers.wrapper.griddb.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peeyush on 3/5/17.
 */

public class GridDBCollections {

    public static List<String> generic = new ArrayList<>();
    public static Map<String, Map<String, String[]> > referenced = new HashMap<>();
    public static List<String> prefixMatched = new ArrayList<>();
    public static String OBSERVATION_COLLECTION = "TS_%s";
    public static String SO_COLLECTION = "TS_SO_%s";


    static {
        //System.out.print("Static Code");
        try{
            JsonParser jsonParser = new JsonParser();
            ClassLoader classLoader = GridDBCollections.class.getClassLoader();
            JsonElement jsonTree = jsonParser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("gdb_collections.json")));
            JsonObject jsonObject = jsonTree.getAsJsonObject();
            JsonArray gen = jsonObject.getAsJsonArray("generic");
            for (int i=0; i<gen.size(); i++)
                generic.add(gen.get(i).getAsString());

            //System.out.println(gen[0]);

            JsonArray ref = jsonObject.getAsJsonArray("referenced");
            for(int i=0;i< ref.size();i++){

                Map<String, String[]> map = new HashMap<>();
                JsonArray mapping = ref.get(i).getAsJsonObject().getAsJsonArray("mapping");

                for(int j=0; j<mapping.size(); j++){
                   JsonArray to =mapping.get(j).getAsJsonObject().getAsJsonArray("to");
                    List<String> temp = new ArrayList<>();
                    for (int k=0; k<to.size(); k++)
                        temp.add(to.get(k).getAsString());

                    map.put(mapping.get(j).getAsJsonObject().get("from").getAsString(), temp.toArray(new String[0]));
                }
               // System.out.println(ref.get(i).getAsJsonObject().get("collection").toString());
                referenced.put(ref.get(i).getAsJsonObject().get("collection").getAsString(), map);

            }
            //System.out.println(generic);
            //System.out.println(referenced);
        }catch (Exception e){

            System.out.println(e);
        }


        /*generic.add("Group");
        generic.add("InfrastructureType");
        generic.add("PlatformType");
        generic.add("Location");
        generic.add("SemanticObservationType");
        generic.add("SensorType");
        generic.add("ObservationType");*/

/*
        Map<String, String[]> map = new HashMap<>();
        map.put("groupIds", new String[]{"Group", "groups"});
        referenced.put("User", map);

        map = new HashMap<>();
        map.put("geometry", new String[]{"Location", "geometry"});
        referenced.put("Region", map);

        map = new HashMap<>();
        map.put("typeId", new String[]{"InfrastructureType", "type"});
        map.put("regionId", new String[]{"Region", "region"});
        referenced.put("Infrastructure", map);

        map = new HashMap<>();
        map.put("locationId", new String[]{"Location", "location"});
        map.put("typeId", new String[]{"PlatformType", "type"});
        map.put("ownerId", new String[]{"User", "owner"});
        referenced.put("Platform", map);

        map = new HashMap<>();
        map.put("locationId", new String[]{"Location", "location"});
        map.put("typeId", new String[]{"SensorType", "type"});
        map.put("platformId", new String[]{"Platform", "platform"});
        map.put("observationTypeId", new String[]{"ObservationType", "observationType"});
        map.put("coverageRoomIds", new String[]{"Infrastructure", "coverage"});

        referenced.put("Sensor", map);
        map = new HashMap<>();
        referenced.put("TS_Observation_", map);
*/


    }

}
