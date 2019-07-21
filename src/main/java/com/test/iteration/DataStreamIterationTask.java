package com.test.iteration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamIterationTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		IterativeStream<Integer> iteration = env.fromElements(1, 2, 3, 4, 5, 7, 8)
				.iterate(1000L);
		DataStream<Integer> decreaseStream = iteration.map(v -> v - 1);
		iteration.closeWith(decreaseStream.filter(v -> v > 0));
		decreaseStream.print();
		env.execute("DataStreamIterationTask");
	}
}
