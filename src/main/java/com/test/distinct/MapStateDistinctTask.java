package com.test.distinct;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 
* 计算广告位的用户点击数。（该例子实现有问题）
* 
* @version V1.0
* @Date 2020年1月13日 下午4:43:01
* @since JDK 1.8
 */
public class MapStateDistinctTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		//广告位id，设备id（用户），时间
		DataStream<Tuple3<Integer, String, Long>> data = env.fromElements(
				Tuple3.of(1, "devId1", 1577808000000L), 
				Tuple3.of(2, "devId2", 1577808000000L),
				Tuple3.of(1, "devId1", 1577808000000L),
				Tuple3.of(2, "devId3", 1577808000000L));
		
		data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Long>>(Time.seconds(10)) {
				private static final long serialVersionUID = 1L;
				@Override
				public long extractTimestamp(Tuple3<Integer, String, Long> element) {
					return element.f2;
				}
			})
			.keyBy(new KeySelector<Tuple3<Integer, String, Long>, Tuple2<Integer, Long>>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<Integer, Long> getKey(Tuple3<Integer, String, Long> value) throws Exception {
					long endTime = TimeWindow.getWindowStartWithOffset(value.f2, 0, Time.minutes(1).toMilliseconds())
							+ Time.minutes(1).toMilliseconds();
					return Tuple2.of(value.f0, endTime);
				}
				
			})
			.process(new MapStateDistinctFunc())
			.print();
		env.execute("MapStateDistinctTask");
	}
}
