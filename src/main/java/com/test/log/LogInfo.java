package com.test.log;

public class LogInfo {

	private String source;

	private long offset;

	private String message;

	private LogMessage logMessage;

	public LogMessage getLogMessage() {
		return logMessage;
	}

	public void setLogMessage(LogMessage logMessage) {
		this.logMessage = logMessage;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "LogInfo [source=" + source + ", offset=" + offset + ", message=" + message + ", logMessage=" + logMessage + "]";
	}

}
