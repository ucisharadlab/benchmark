/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.util.metadata.declared;

import edu.uci.ics.tippers.common.exceptions.AlgebricksException;
import edu.uci.ics.tippers.tql.util.metadata.entities.Dataset;
import edu.uci.ics.tippers.tql.util.metadata.entities.Dataverse;

import java.util.Map;

public class MetadataProvider {


    private final Dataverse defaultDataverse;

    private boolean isWriteTransaction;
    private Map<String, String> config;
    private boolean asyncResults;
    private Map<String, Integer> locks;
    private boolean isTemporaryDatasetWriteJob = true;
    private boolean blockingOperatorDisabled = false;

    public MetadataProvider(Dataverse defaultDataverse) {
        this.defaultDataverse = defaultDataverse;
    }

    public String getPropertyValue(String propertyName) {
        return config.get(propertyName);
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public void disableBlockingOperator() {
        blockingOperatorDisabled = true;
    }

    public boolean isBlockingOperatorDisabled() {
        return blockingOperatorDisabled;
    }

    public Map<String, String> getConfig() {
        return config;
    }


    public Dataverse getDefaultDataverse() {
        return defaultDataverse;
    }

    public String getDefaultDataverseName() {
        return defaultDataverse == null ? null : defaultDataverse.getDataverseName();
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.isWriteTransaction = writeTransaction;
    }

    public boolean getResultAsyncMode() {
        return asyncResults;
    }

    public void setResultAsyncMode(boolean asyncResults) {
        this.asyncResults = asyncResults;
    }

    public boolean isWriteTransaction() {
        // The transaction writes persistent datasets.
        return isWriteTransaction;
    }

    public boolean isTemporaryDatasetWriteJob() {
        // The transaction only writes temporary datasets.
        return isTemporaryDatasetWriteJob;
    }

    public Map<String, Integer> getLocks() {
        return locks;
    }

    public void setLocks(Map<String, Integer> locks) {
        this.locks = locks;
    }

    public Dataset findDataset(String dataverse, String dataset) throws AlgebricksException {
        String dv =
                dataverse == null ? (defaultDataverse == null ? null : defaultDataverse.getDataverseName()) : dataverse;
        if (dv == null) {
            return null;
        }
        return null;
    }

}