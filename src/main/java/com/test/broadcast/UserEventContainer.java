package com.test.broadcast;

import java.util.ArrayList;
import java.util.List;


public class UserEventContainer {

	private String userId;

	private List<UserEvent> userEvents = new ArrayList<>();

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
