package com.test.processfunc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionTest {

	public static void main(String[] args) {
		// 定义源数据流
		DataStream<Tuple2<String, String>> stream = null;

		// 将 process function 应用到一个键控流(keyed stream)中
		DataStream<Tuple2<String, Long>> result = stream.keyBy(new MyKeySelector()).process(new CountWithTimeoutFunction());
	}

	static class MyKeySelector implements KeySelector<Tuple2<String, String>, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, String> value) throws Exception {
			return value.f0;
		}

	}

	static class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

		private static final long serialVersionUID = 1L;

		/** The state that is maintained by this process function */
		/** process function维持的状态 */
		private ValueState<CountWithTimestamp> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
		}

		@Override
		public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
			// retrieve the current count
			// 获取当前的count
			CountWithTimestamp current = state.value();
			if (current == null) {
				current = new CountWithTimestamp();
				current.key = value.f0;
			}

			// update the state's count
			// 更新 state 的 count
			current.count++;

			// set the state's timestamp to the record's assigned event time timestamp
			// 将state的时间戳设置为记录的分配事件时间戳
			current.lastModified = ctx.timestamp();

			// write the state back
			// 将状态写回
			state.update(current);

			// schedule the next timer 60 seconds from the current event time
			// 从当前事件时间开始计划下一个60秒的定时器
			ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

			// get the state for the key that scheduled the timer
			// 获取计划定时器的key的状态
			CountWithTimestamp result = state.value();

			// 检查是否是过时的定时器或最新的定时器
			if (timestamp == result.lastModified + 60000) {
				// emit the state on timeout
				out.collect(new Tuple2<String, Long>(result.key, result.count));
			}
		}
	}

	static class CountWithTimestamp {

		public String key;

		public long count;

		public long lastModified;
	}

}