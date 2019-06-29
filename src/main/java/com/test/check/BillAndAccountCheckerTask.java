package com.test.check;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class BillAndAccountCheckerTask {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "10.7.30.22:9092");
		props.setProperty("group.id", "check");
		FlinkKafkaConsumer<String> billConsumer = new FlinkKafkaConsumer<>("bill", new SimpleStringSchema(), props);
		FlinkKafkaConsumer<String> frontAccountConsumer = new FlinkKafkaConsumer<>("frontAccount", new SimpleStringSchema(), props);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStream<TransactionInfo> billStream = env.addSource(billConsumer)
				.map(new FrontAccountDetailMap())
				.assignTimestampsAndWatermarks(new BillInfoTimestampExtractor(Time.seconds(30)))
				.keyBy(e -> e.getBusinessRecordNumber());
		
		DataStream<TransactionInfo> frontAccountDetailStream = env.addSource(frontAccountConsumer)
				.map(new BillMap())
				.assignTimestampsAndWatermarks(new AccountInfoTimestampExtractor(Time.seconds(30)))
				.keyBy(e -> e.getBusinessRecordNumber());
		
		billStream.coGroup(frontAccountDetailStream)
			.where(e -> e.getBusinessRecordNumber())
			.equalTo(e -> e.getBusinessRecordNumber())
			.window(TumblingEventTimeWindows.of(Time.seconds(60), Time.hours(-8)))
			.apply(new LeftCoGroup())
			.addSink(new MySQLSink());
		
		env.execute("BillAndAccountCheckerTask");
	}

}