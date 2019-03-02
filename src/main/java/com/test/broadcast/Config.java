package com.test.broadcast;

public class Config {

	private String channel;

	private String registerDate;

	private int historyPurchaseTimes;

	private int maxPurchasePathLength;

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getRegisterDate() {
		return registerDate;
	}

	public void setRegisterDate(String registerDate) {
		this.registerDate = registerDate;
	}

	public int getHistoryPurchaseTimes() {
		return historyPurchaseTimes;
	}

	public void setHistoryPurchaseTimes(int historyPurchaseTimes) {
		this.historyPurchaseTimes = historyPurchaseTimes;
	}

	public int getMaxPurchasePathLength() {
		return maxPurchasePathLength;
	}

	public void setMaxPurchasePathLength(int maxPurchasePathLength) {
		this.maxPurchasePathLength = maxPurchasePathLength;
	}

}
