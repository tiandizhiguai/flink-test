package com.test.log;

public class LogMessage {

	private String date;

	private String time;

	private String thread;

	private String infoLevel;

	private String clazz;

	private String info;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getThread() {
		return thread;
	}

	public void setThread(String thread) {
		this.thread = thread;
	}

	public String getInfoLevel() {
		return infoLevel;
	}

	public void setInfoLevel(String infoLevel) {
		this.infoLevel = infoLevel;
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	@Override
	public String toString() {
		return "[date=" + date + ", time=" + time + ", thread=" + thread + ", infoLevel=" + infoLevel + ", clazz=" + clazz + ", info="
				+ info + "]";
	}

}
