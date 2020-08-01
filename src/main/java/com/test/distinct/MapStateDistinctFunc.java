package com.test.distinct;

import java.io.IOException;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * 计算广告位的用户点击数。
 * 
 * @version V1.0
 * @Date 2020年1月13日 下午4:43:01
 * @since JDK 1.8
 */
public class MapStateDistinctFunc extends KeyedProcessFunction<Tuple2<Integer, Long>,
	Tuple3<Integer, String, Long>, Tuple2<String, Long>> {

	private static final Logger logger = LoggerFactory.getLogger(MapStateDistinctFunc.class);
	
	private static final long serialVersionUID = 1L;
	
	private MapStateDescriptor<String, Integer> devIdStateDesc;
	
	private MapState<String, Integer> devIdState;
	
	private ValueStateDescriptor<Long> countStateDesc;
	
	private ValueState<Long> countState;

	@Override
	public void open(Configuration conf) throws Exception {
		devIdStateDesc = new MapStateDescriptor<String, Integer>("devIdState", StringSerializer.INSTANCE, IntSerializer.INSTANCE);
		devIdState = this.getRuntimeContext().getMapState(devIdStateDesc);
		countStateDesc = new ValueStateDescriptor<Long>("countState", LongSerializer.INSTANCE);
		countState = this.getRuntimeContext().getState(countStateDesc);
	}

	@Override
	public void processElement(Tuple3<Integer, String, Long> value, Context context, 
			Collector<Tuple2<String, Long>> collector) throws Exception {
		long currentW = context.timerService().currentWatermark();
		if(context.getCurrentKey().f1 + 1 <= currentW) {
			logger.warn("late data : {}", value);
		}
		
		Integer devCount = devIdState.get(value.f1);
		if(devCount == null) {
			devIdState.put(value.f1, 1);
			Long v = countState.value();
			countState.update(v == null ? 1 : ++v);
			
			//还需要注册一个定时器
			context.timerService().registerEventTimeTimer(context.getCurrentKey().f1 + 1);
		}
	}
	
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
		devIdState.entries().forEach(e -> {
			try {
				out.collect(Tuple2.of(e.getKey(), countState.value()));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		});
		devIdState.clear();
		countState.clear();
	}
}