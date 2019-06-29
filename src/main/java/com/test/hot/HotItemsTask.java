package com.test.hot;

import java.io.File;
import java.net.URL;

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class HotItemsTask {

	private static final String[] FIELDS = new String[] { "userId", "itemId", "categoryId", "behavior", "timestamp" };

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 为了打印到控制台的结果不乱序，我们配置全局的并发为 1，这里改变并发对结果正确性没有影响
		env.setParallelism(1);

		URL fileUrl = HotItemsTask.class.getClassLoader().getResource("UserBehavior.csv");
		Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
		PojoTypeInfo<UserBehavior> typeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		//PojoTypeInfo<UserBehavior> typeInfo2 = (PojoTypeInfo<UserBehavior>) TypeInformation.of(UserBehavior.class);
		
		CsvInputFormat<UserBehavior> inputFormat = new PojoCsvInputFormat<>(filePath, typeInfo, FIELDS);
		env.createInput(inputFormat, typeInfo)
			.assignTimestampsAndWatermarks(new TimeExtractor())
			.filter(e -> "pv".equals(e.behavior))
			.keyBy(e -> e.itemId)
			.window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5), Time.hours(-8)))
			.aggregate(new CountAgg(), new ResultWindow())
			.keyBy(e -> e.windowEnd)
			.process(new HotItemsProcess(3))
			.print();
		env.execute("HotItemsTask");
	}
}
