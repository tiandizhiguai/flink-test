package com.test.broadcast;

import java.util.Map;

public class EvaluatedResult {

	private String userId;

	private String channel;

	private int purchasePathLen;

	private Map<String, Integer> eventTypeCounts;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public int getPurchasePathLen() {
		return purchasePathLen;
	}

	public void setPurchasePathLen(int purchasePathLen) {
		this.purchasePathLen = purchasePathLen;
	}

	public Map<String, Integer> getEventTypeCounts() {
		return eventTypeCounts;
	}

	public void setEventTypeCounts(Map<String, Integer> eventTypeCounts) {
		this.eventTypeCounts = eventTypeCounts;
	}

	@Override
	public String toString() {
		return "EvaluatedResult [userId=" + userId + ", channel=" + channel + ", purchasePathLen=" + purchasePathLen + ", eventTypeCounts="
				+ eventTypeCounts + "]";
	}

}
