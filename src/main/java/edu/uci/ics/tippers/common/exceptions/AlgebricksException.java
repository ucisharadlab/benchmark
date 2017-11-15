/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.exceptions;


import java.io.Serializable;

public class AlgebricksException extends Exception {
    private static final long serialVersionUID = 1L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private transient volatile String msgCache;

    public AlgebricksException(String component, int errorCode, String message, Throwable cause, String nodeId,
                               Serializable... params) {
        super(message, cause);
        this.component = component;
        this.errorCode = errorCode;
        this.nodeId = nodeId;
        this.params = params;
    }


    public AlgebricksException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, params);
    }

    public AlgebricksException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, null, params);
    }

    public AlgebricksException(String component, int errorCode, String message, Throwable cause,
                               Serializable... params) {
        this(component, errorCode, message, cause, null, params);
    }

    public String getComponent() {
        return component;
    }

    public Object[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

}
