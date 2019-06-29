package com.test.hot;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class TimeExtractor extends AscendingTimestampExtractor<UserBehavior> {

	private static final long serialVersionUID = 1L;

	@Override
	public long extractAscendingTimestamp(UserBehavior element) {
		return element.timestamp;
	}

}
