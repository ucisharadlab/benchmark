package edu.uci.ics.tippers.tql.util.metadata;

/**
 * Contains metadata constants
 */
public class MetadataConstants {

    // Name of the dataverse the metadata lives in.
    public static final String METADATA_DATAVERSE_NAME = "Metadata";

    // Name of the node group where metadata is stored on.
    public static final String METADATA_NODEGROUP_NAME = "MetadataGroup";

    // Name of the default nodegroup where internal/feed datasets will be partitioned
    // if an explicit nodegroup is not specified at the time of creation of a dataset
    public static final String METADATA_DEFAULT_NODEGROUP_NAME = "DEFAULT_NG_ALL_NODES";

    private MetadataConstants() {
    }
}
