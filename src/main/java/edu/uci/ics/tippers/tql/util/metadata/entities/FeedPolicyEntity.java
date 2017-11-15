/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.util.metadata.entities;

import edu.uci.ics.tippers.tql.util.FeedPolicy;

import java.util.Map;

/**
 * Metadata describing a feed activity record.
 */
public class FeedPolicyEntity extends FeedPolicy {

    public FeedPolicyEntity(String dataverseName, String policyName, String description,
                            Map<String, String> properties) {
        super(dataverseName, policyName, description, properties);
    }

    private static final long serialVersionUID = 1L;

}
