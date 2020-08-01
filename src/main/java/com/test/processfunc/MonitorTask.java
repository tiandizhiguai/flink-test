package com.test.processfunc;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 
 * 服务器下线监控报警，服务器上下线都会发送一条消息，如果发送的是下线消息，在之后的1秒内没有收到上线消息则循环发出警告，直到上线取消告警。 实现思路： 1.由于根据服务器不在线时间来告警，应该使用ProcessingTime语义
 * 2.首先将服务器信息按照serverId分组，然后使用一个继承KeyedProcessFunction的类的Function接受处理，定义两个ValueState分别存储触发时间与服务器信息， open方法，初始化状态信息
 * processElement方法，处理每条流入的数据，如果收到的是offline状态，则注册一个ProcessingTime的定时器，并且将服务器信息与定时时间存储状态中；如果收到的是online状态并且状态中定时时间不为-1，则删除定时器并将状态时间置为-1
 * onTimer方法，定时回调的方法，触发报警并且注册下一个定时告警
 * 
 */
public class MonitorTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<ServerMsg> data = env.fromElements(new ServerMsg("server1", false, 1572600246430L), new ServerMsg("server1", false, 1572600248430L),
				new ServerMsg("server1", true, 1572600249430L));
		data.assignTimestampsAndWatermarks(new TimestampExtractor(Time.seconds(5)))
			.keyBy(e -> e.getServerId())
			.process(new MonitorKeyedProcessFunction())
			.print();
		env.execute("MonitorTask");
	}
}
