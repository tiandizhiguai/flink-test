package com.test.broadcast;

import java.util.List;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;

public class UserEventContainer {

	private String userId;

	private List<UserEvent> userEvents = Lists.newArrayList();

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public List<UserEvent> getUserEvents() {
		return userEvents;
	}

	public void addUserEvent(UserEvent userEvent) {
		this.userEvents.add(userEvent);
	}

}
