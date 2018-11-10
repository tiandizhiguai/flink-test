package com.test.log;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class LogAnalyzer {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "10.7.30.22:9092");
		props.setProperty("group.id", "log-analyzer");
		FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);
		consumer.setStartFromEarliest();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		SingleOutputStreamOperator<Tuple2<String, Integer>> classCount = env.addSource(consumer)
				.flatMap(new LogSplitter())
				.keyBy(new ClassKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.apply(new LogWindowFunction());

		classCount.print();
		env.execute("LogAnalyzer");
	}

}