package com.test.state;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 利用state进行数据去重，数据格式为： user、item、catelog、behavior、timestamp，
 * 其中第一个数据和最后一行数据视为重复数据（两次点击间隔一秒）。
 */
public class DataDeduplicateTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<String> dataStream = env.fromElements("952483,310884,4580532,pv,1511712000", "794777,5119439,982926,pv,1511712000",
				"875914,4484065,1320293,pv,1511712000", "980877,5097906,149192,pv,1511712000", "944074,2348702,3002561,pv,1511712000",
				"973127,1132597,4181361,pv,1511712000", "84681,3505100,2465336,pv,1511712000", "732136,3815446,2342116,pv,1511712000",
				"940143,2157435,1013319,pv,1511712000", "655789,4945338,4145813,pv,1511712000", "952483,310884,4580532,pv,1511713000");
		
		DataStream<Object> process = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {
				String[] split = s.split(",");
				if ("pv".equals(split[3])) {
					Tuple2<String, Long> res = new Tuple2<>(split[0] + "-" + split[1], Long.parseLong(split[4]));
					out.collect(res);
				}
			}
		}).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>> forBoundedOutOfOrderness(Duration.ofMillis(1000))
				.withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (s, l) -> s.f1)).keyBy(s -> s.f0)
				.process(new KeyedProcessFunction<String, Tuple2<String, Long>, Object>() {
					private static final long serialVersionUID = 1L;
					private ValueState<UserBehavior> state;

					@Override
					public void open(Configuration parameters) {
						ValueStateDescriptor<UserBehavior> stateDescriptor = new ValueStateDescriptor<>("mystate", UserBehavior.class);
						stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build());
						state = getRuntimeContext().getState(stateDescriptor);
					}

					@Override
					public void processElement(Tuple2<String, Long> in, Context ctx, Collector<Object> out) throws Exception {
						UserBehavior cur = state.value();
						if (cur == null) {
							cur = new UserBehavior(in.f0, in.f1);
							state.update(cur);
							ctx.timerService().registerEventTimeTimer(cur.getTimestamp() + 60000);
							out.collect(cur);
						} else {
							System.out.println("[Duplicate Data] " + in.f0 + " " + in.f1);
						}
					}

					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
						UserBehavior cur = state.value();
						if (cur.getTimestamp() + 1000 <= timestamp) {
							System.out.printf("[Overdue] now: %d obj_time: %d Date: %s%n", timestamp, cur.getTimestamp(), cur.getId());
							state.clear();
						}
					}
				});
		
		process.print();
		
		env.execute("flink");
	}

	private static class UserBehavior {

		private String id;

		private long timestamp;

		public UserBehavior(String id, long timestamp) {
			super();
			this.id = id;
			this.timestamp = timestamp;
		}

		public String getId() {
			return id;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "UserBehavior [id=" + id + ", timestamp=" + timestamp + "]";
		}
		
	}
}
