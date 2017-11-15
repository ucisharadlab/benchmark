/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.util.metadata.entities;

/**
 * Metadata describing a dataverse.
 */
public class Dataverse  {

    private static final long serialVersionUID = 1L;
    // Enforced to be unique within an Asterix cluster..
    private final String dataverseName;
    private final String dataFormat;
    private final int pendingOp;

    public Dataverse(String dataverseName, String format, int pendingOp) {
        this.dataverseName = dataverseName;
        this.dataFormat = format;
        this.pendingOp = pendingOp;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public int getPendingOp() {
        return pendingOp;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + dataverseName;
    }
}
