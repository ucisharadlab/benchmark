package edu.uci.ics.tippers.model.metadata.user;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;
import java.util.List;

public class User extends SemanticEntity {

	private String name;
	
	private String googleAuthToken;

	private List<UserGroup> groups;

	private String emailId;

    public User() {
        super();
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

	public String getEmailId() {
		return emailId;
	}

	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}
}
