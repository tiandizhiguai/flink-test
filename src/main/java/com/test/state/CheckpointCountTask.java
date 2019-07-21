package com.test.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointCountTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(10L);
		env.fromElements(Tuple2.of("a", 3L), Tuple2.of("b", 5L), Tuple2.of("a", 7L), Tuple2.of("b", 4L), Tuple2.of("c", 2L))
			.keyBy(0)
	        .flatMap(new CheckpointCount())
	        .print();
		env.execute("CheckpointCountTask");
	}
}
