package edu.uci.ics.tippers.model.platform;

import edu.uci.ics.tippers.model.metadata.user.User;

/**
 * The Class Platform to model platforms containing sensors (e.g., a smartphone
 * or a raspberry pi)
 */
public class Platform {
	
	private String id;

	private String name;

	private User owner;

	private PlatformType type_;

	private String hashedMac;

	public String getHashedMac() {
		return hashedMac;
	}

	public void setHashedMac(String hashedMac) {
		this.hashedMac = hashedMac;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public User getOwner() {
		return owner;
	}

	public void setOwner(User owner) {
		this.owner = owner;
	}

	public PlatformType getType_() {
		return type_;
	}

	public void setType_(PlatformType type_) {
		this.type_ = type_;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
    public String toString() {
        return "Platform [id=" + id +
                ", type_=" + type_ +
                ", owner=" + ((owner == null)? "null":owner.getId()) +
                "]";
    }

}
