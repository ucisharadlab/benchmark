package edu.uci.ics.tippers.model.metadata.spatial;

public class Location{

	private String id;
	
	double x;
	
	double y;
	
	double z;

	public Location() {

	}

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

	public Location(double x, double y, double z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	/* (non-Javadoc)
         * @see java.awt.Point#equals(java.lang.Object)
         * Overriding equals to check if two locations are the same
         */
	@Override
    public boolean equals(Object obj) {
		if (!(obj instanceof Location))
			return false;
		if (obj == this)
			return true;

		Location loc = (Location) obj;
		if(loc.x==this.x && loc.y==this.y && loc.z==this.z)
			return true;

		return false;
    }

	@Override
	public String toString() {
		return "Location [id=" + id + ", x=" + x + ", y=" + y + ", z=" + z + "]";
	}

}
