package com.test.sideoutput;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputProcessFunction extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {

	private static final long serialVersionUID = 1L;
	
	private OutputTag<String> rejectedTag;
	
	public SideOutputProcessFunction(OutputTag<String> rejectedTag) {
		this.rejectedTag = rejectedTag;
	}

	@Override
	public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
		if (value.length() > 4) {
			context.output(rejectedTag, value);
		} else {
			collector.collect(new Tuple2<>(value, 1));
		}
	}
}