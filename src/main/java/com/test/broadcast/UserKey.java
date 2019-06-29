package com.test.broadcast;

import org.apache.flink.api.java.functions.KeySelector;

public class UserKey implements KeySelector<UserEvent, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String getKey(UserEvent value) throws Exception {
		return value.getUserId();
	}

}
