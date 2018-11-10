package com.test.account;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingEventTimeWindowAnalyzer {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		Properties properties = new Properties();
		properties.put("consumerGroup", "accountingConsumerGroup");
		properties.put("namesrvAddr", "rocketmq-server-vip:9876");
		List<String> topics = new ArrayList<>();
		topics.add("account-acd");
		SingleOutputStreamOperator<Tuple2<String, BigDecimal>> datas = env.addSource(new RocketmqConsumer(topics, properties))
				.flatMap(new AccountingInfoParser())
				.assignTimestampsAndWatermarks(new AccountingInfoTimestampExtractor(Time.seconds(60)))
				.keyBy(new AccountingInfoKeySelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.process(new AccountingInfoProcessWindowFunction());
		datas.print();
		env.execute("AccountAnalyzer");
	}

}