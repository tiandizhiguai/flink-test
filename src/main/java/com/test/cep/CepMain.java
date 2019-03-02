package com.test.cep;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CepMain {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Event> input = env.fromElements(Tuple2.of(new Event(1, "start", 1.0), 5L), Tuple2.of(new Event(2, "middle", 2.0), 1L),
				Tuple2.of(new Event(3, "end", 3.0), 3L), Tuple2.of(new Event(4, "end", 4.0), 10L), // 触发2，3，1
				Tuple2.of(new Event(5, "middle", 5.0), 7L), Tuple2.of(new Event(5, "middle", 5.0), 100L) // 触发5，4
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long previousTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		});

		Pattern<Event, Event> pattern = Pattern.<Event> begin("start").where(new SimpleCondition<Event>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}

		}).followedByAny("end").where(new SimpleCondition<Event>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}

		});

		CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String select(Map<String, List<Event>> pattern) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append("=========================");
				for (Entry<String, List<Event>> entry : pattern.entrySet()) {
					sb.append(entry.getValue().get(0).getOrder()).append(",");
				}
				return sb.toString();
			}

		}).print();

		env.execute("CepMain");
	}
}
