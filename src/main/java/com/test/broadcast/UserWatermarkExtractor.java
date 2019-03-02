package com.test.broadcast;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UserWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

	private static final long serialVersionUID = 1928099150527367225L;

	public UserWatermarkExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(UserEvent element) {
		return element.getEventTimestamp();
	}

}
