package edu.uci.ics.tippers.model.metadata.infrastructure;

public class Location{

	private String id;
	
	double x;
	
	double y;
	
	double z;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getZ() {
		return z;
	}

	public void setZ(double z) {
		this.z = z;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Location [id=" + id + ", x=" + x + ", y=" + y + ", z=" + z + "]";
	}

}
