package edu.uci.ics.tippers.model.metadata.spatial;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peeyush on 17/4/17.
 */
public class Region {

    private String id;

    private String name;

    private List<Location> geometry = new ArrayList<>();

    private double floor;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Location> getGeometry() {
        return geometry;
    }

    public void setGeometry(List<Location> geometry) {
        this.geometry = geometry;
    }

    public double getFloor() {
        return floor;
    }

    public void setFloor(double floor) {
        this.floor = floor;
    }
}
