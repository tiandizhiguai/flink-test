package com.test.trigger;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 
* 带计数的时间窗口例子2：
* 1.窗口达到5秒时，触发窗口；
* 2.当元素数量达到2时，触发窗口；
* 
* @version V1.0
* @Date 2020年1月16日 上午10:17:19
* @since JDK 1.8
 */
public class CountWithTimeoutTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Long>> data = env.fromElements(
				Tuple2.of("class1", 1573874530000L),
				Tuple2.of("class1", 1573874530000L),
				Tuple2.of("class1", 1573874531000L),
				Tuple2.of("class1", 1573874531000L),
				Tuple2.of("class1", 1573874540000L));
		data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(2)) {
				private static final long serialVersionUID = 1L;
				@Override
				public long extractTimestamp(Tuple2<String, Long> element) {
					return element.f1;
				}
			})
			.timeWindowAll(Time.seconds(5))
			.trigger(new CountTriggerWithTimeout<Tuple2<String, Long>>(2, TimeCharacteristic.EventTime))
			.process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>(){
				private static final long serialVersionUID = 1L;
				public void process(Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
					System.out.println("start =============================");
					elements.forEach(e -> System.out.println(e));
					System.out.println("end =============================");
				}
			});
		env.execute("CountWithTimeoutTask2");
	}
}