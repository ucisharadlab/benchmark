package edu.uci.ics.tippers.wrapper.griddb.util;

public class Error {
	
	int code;
	String message;
	String fields;

	public Error(int code, String message, String fields) {
		this.code = code;
		this.message = message;
		this.fields = fields;
	
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return "Error [code=" + code + ", message=" + message + ", fields=" + fields + "]";
	}
	
	

}
