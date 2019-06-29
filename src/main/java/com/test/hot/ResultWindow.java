package com.test.hot;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ResultWindow implements WindowFunction<Long, ItemViewCount, String, TimeWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
		Long count = input.iterator().next();
		out.collect(ItemViewCount.of(key, window.getEnd(), count));
	}

}
