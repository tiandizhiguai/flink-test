package com.test.sideoutput;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * 
* 单词长度小于4的单词进行wordcount操作，同时打印长度大于4的单词。
* 
* @version V1.0
* @Date 2019年7月21日 下午9:25:41
* @since JDK 1.8
 */
public class SideOutputTask {

	private static final OutputTag<String> rejectedTag = new OutputTag<String>("rejected") {
		private static final long serialVersionUID = 1L;
	};
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = env.fromElements("hello", "world", "i", "love", "you")
			.keyBy(e -> e)
			.process(new SideOutputProcessFunction(rejectedTag));
		tokenized.getSideOutput(rejectedTag)
			.map(e -> "rejected-" + e)
			.print();
		tokenized.keyBy(0)
			.sum(1)
			.print();
		env.execute("SideOutputTask");
	}
}
