package com.test.async;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.test.async.MySQLAsyncSink;
import com.test.async.Protocol;

public class MySQLAsyncSinkTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> stream = env.fromElements("A", "B");
		SingleOutputStreamOperator<Protocol> asyncStream = AsyncDataStream.unorderedWait(stream, new MySQLAsyncSink(), 10L, TimeUnit.SECONDS);
		asyncStream.print();
		env.execute("MySQLAsyncSinkTest");
	}
}
