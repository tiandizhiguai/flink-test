package com.test.log;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LogWindowFunction implements WindowFunction<LogInfo, Tuple2<String, Integer>, String, TimeWindow> {

	private static final long serialVersionUID = 3919245025545286500L;

	@Override
	public void apply(String key, TimeWindow window, Iterable<LogInfo> input, Collector<Tuple2<String, Integer>> out) throws Exception {
		int sum = 0;
		String className = "";
		for (LogInfo logInfo : input) {
			className = logInfo.getLogMessage().getClazz();
			sum++;
		}
		out.collect(new Tuple2<String, Integer>(className, sum));
	}

}
