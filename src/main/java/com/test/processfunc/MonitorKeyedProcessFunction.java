package com.test.processfunc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 服务器下线监控报警，服务器上下线都会发送一条消息，如果发送的是下线消息，在之后的1秒内没有收到上线消息则循环发出警告，直到上线取消告警。 实现思路： 1.由于根据服务器不在线时间来告警，应该使用ProcessingTime语义
 * 2.首先将服务器信息按照serverId分组，然后使用一个继承KeyedProcessFunction的类的Function接受处理，定义两个ValueState分别存储触发时间与服务器信息， open方法，初始化状态信息
 * processElement方法，处理每条流入的数据，如果收到的是offline状态，则注册一个ProcessingTime的定时器，并且将服务器信息与定时时间存储状态中；如果收到的是online状态并且状态中定时时间不为-1，则删除定时器并将状态时间置为-1
 * onTimer方法，定时回调的方法，触发报警并且注册下一个定时告警
 * 
 */
public class MonitorKeyedProcessFunction extends KeyedProcessFunction<String, ServerMsg, String> {

	private static final Logger logger = LoggerFactory.getLogger(MonitorKeyedProcessFunction.class);

	private static final long serialVersionUID = 1L;

	private ValueState<Long> timeState;

	private ValueState<String> serverState;

	@Override
	public void open(Configuration parameters) throws Exception {
		timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", TypeInformation.of(Long.class)));
		serverState = getRuntimeContext().getState(new ValueStateDescriptor<String>("server-state", TypeInformation.of(String.class)));
	}

	@Override
	public void processElement(ServerMsg value, Context context, Collector<String> collector) throws Exception {
		if (!value.isOnline()) {
			long monitorTime = value.getTimestamp() + 1000;
			timeState.update(monitorTime);
			serverState.update(value.getServerId());
			context.timerService().registerEventTimeTimer(monitorTime);
		}else if (value.isOnline() && timeState.value() != -1L) {
			context.timerService().deleteEventTimeTimer(timeState.value());
			timeState.update(-1L);
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
		if (timestamp == timeState.value()) {
			logger.info("告警:" + serverState.value() + " is offline, please restart");
			
			long newMonitorTime = timestamp + 1000;
			timeState.update(newMonitorTime);
			ctx.timerService().registerEventTimeTimer(newMonitorTime);
		}
	}

}

class ServerMsg {

	private String serverId;

	private boolean isOnline;

	private long timestamp;

	public ServerMsg(String serverId, boolean isOnline, long timestamp) {
		super();
		this.serverId = serverId;
		this.isOnline = isOnline;
		this.timestamp = timestamp;
	}

	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}

	public boolean isOnline() {
		return isOnline;
	}

	public void setOnline(boolean isOnline) {
		this.isOnline = isOnline;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
