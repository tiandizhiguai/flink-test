package com.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class FlinkTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.disableOperatorChaining();
		env.setParallelism(1);
		OutputTag<String> tag = new OutputTag<String>("lateData");
		DataStream<Tuple2<String, Integer>> dataStream1 = env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("c", 3));
		DataStream<Tuple2<String, Integer>> dataStream2 = env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("c", 3));
		dataStream1.join(dataStream2)
			.where(e1 -> e1.f0)
			.equalTo(e2 -> e2.f0)
			.window(TumblingEventTimeWindows.of(Time.seconds(30)));
			//.apply(function);
		
		
		env.execute("FlinkTest");
	}
}
