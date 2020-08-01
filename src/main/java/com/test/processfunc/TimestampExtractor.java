package com.test.processfunc;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ServerMsg> {

	private static final long serialVersionUID = -5504853504737136873L;

	public TimestampExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}	

	@Override
	public long extractTimestamp(ServerMsg element) {
		return element.getTimestamp();
	}
	
}
