package com.test.distinct;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 
* 利用去重算法计算一段时间内设备的数量。
* 
* @version V1.0
* @Date 2020年1月13日 下午4:43:01
* @since JDK 1.8
 */
public class DistinctTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		DataStream<Tuple3<Integer, String, Long>> data = env.fromElements(
				Tuple3.of(1, "devId1", 1577808000000L), 
				Tuple3.of(1, "devId2", 1577808000000L),
				Tuple3.of(1, "devId1", 1577808000000L),
				Tuple3.of(1, "devId3", 1577808000000L));
		
		data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Long>>(Time.seconds(10)) {
				private static final long serialVersionUID = 1L;
				@Override
				public long extractTimestamp(Tuple3<Integer, String, Long> element) {
					return element.f2;
				}
			})
			.keyBy(2)
			.timeWindow(Time.seconds(10))
			//.aggregate(new HLLDistinctFunc())
			.aggregate(new BitMapDistinctFunc())
			.map(e -> "===================================设备数量 ：" + e)
			.print();
		env.execute("DistinctTask");
	}
}
