package edu.uci.ics.tippers.wrapper.griddb.util;

/**
 * The Enum HTTPStatusCode.
 */
public enum HTTPStatusCode {

	/** The status ok. */
	STATUS_OK(200), 
	
	/** The status created. */
	STATUS_CREATED(201),
	
	/** The status badrequest. */
	STATUS_BADREQUEST(401),
	
	STATUS_CONFLICT(409);
	
	/** The value. */
	private int value;
	
	/**
	 * Instantiates a new HTTP status code.
	 *
	 * @param value the value
	 */
	private HTTPStatusCode(int value) {
		this.value = value;
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public int getValue() {
		return value;
	}
}
