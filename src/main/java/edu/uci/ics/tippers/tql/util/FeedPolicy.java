/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.util;

import java.io.Serializable;
import java.util.Map;

public class FeedPolicy implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String policyName;
    // A description of the policy
    private final String description;
    // The policy properties associated with the feed dataset
    private Map<String, String> properties;

    public FeedPolicy(String dataverseName, String policyName, String description, Map<String, String> properties) {
        this.dataverseName = dataverseName;
        this.policyName = policyName;
        this.description = description;
        this.properties = properties;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getPolicyName() {
        return policyName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeedPolicy)) {
            return false;
        }
        FeedPolicy otherPolicy = (FeedPolicy) other;
        if (!otherPolicy.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherPolicy.policyName.equals(policyName)) {
            return false;
        }
        return true;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
