package edu.uci.ics.tippers.model.metadata.user;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;
import edu.uci.ics.tippers.model.platform.Platform;
import edu.uci.ics.tippers.model.sensor.Sensor;

import java.util.ArrayList;
import java.util.List;

public class User extends SemanticEntity {

	private String name;
	
	/** Google authentication token */
	private String googleAuthToken;

	/** The groups the user belongs to (e.g., ISG, CMU). */
	private List<UserGroup> groups;

	private List<Sensor> sensors;

	private List<Platform> platforms;

	private String emailId;

	/*
	 * TODO: return platforms, sensors, groups in toString
	 *
	 */
	public User(String name, String googleAuthToken, String email, List<UserGroup> groups, List<Platform> platforms,
                List<Sensor> sensors) {
		super();
		this.name = name;
		this.googleAuthToken = googleAuthToken;
		this.groups = groups;
	}

	public User() {
		this.groups = new ArrayList<UserGroup>();
	}

	public void setGroups(List<UserGroup> groups) {
		this.groups = groups;
	}

	public String getName() {
		return this.name;
	}

	public List<UserGroup> getGroups() {
		return this.groups;
	}


	public void setName(String name) {
		this.name = name;
	}

	/* TODO : Check whether a user's input is correctly formed or not */
	public boolean validate() {
		return true;
	}
	
	/* TODO : Checks whether a user existed in database */
	public boolean exists() {
		return true;
	}

	@Override
	public String toString() {

		return "User [name=" + name + ", googleAuthToken=" + googleAuthToken + "]";
	}

	public String getGoogleAuthToken() {
		return googleAuthToken;
	}

	public void setGoogleAuthToken(String googleAuthToken) {
		this.googleAuthToken = googleAuthToken;
	}

	public List<Sensor> getSensors() {
		return sensors;
	}

	public void setSensors(List<Sensor> sensors) {
		this.sensors = sensors;
	}

	public List<Platform> getPlatforms() {
		return platforms;
	}

	public void setPlatforms(List<Platform> platforms) {
		this.platforms = platforms;
	}

	public String getEmailId() {
		return emailId;
	}

	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}
}
