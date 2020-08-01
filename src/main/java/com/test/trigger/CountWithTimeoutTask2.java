package com.test.trigger;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountWithTimeoutTask2 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		
		DataStream<Tuple2<String, Long>> data = env.fromElements(
				Tuple2.of("class1", 1573874530000L),
				Tuple2.of("class1", 1573874530000L),
				Tuple2.of("class1", 1573874531000L),
				Tuple2.of("class1", 1573874532000L),
				Tuple2.of("class1", 1573874533000L));
		data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(2)) {
				private static final long serialVersionUID = 1L;
				@Override
				public long extractTimestamp(Tuple2<String, Long> element) {
					return element.f1;
				}
			})
			.keyBy(t -> t.f0)
			.window(GlobalWindows.create())
			.trigger(new BatchSendTrigger<Tuple2<String, Long>>(10, 1000))
			.process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
				private static final long serialVersionUID = 1L;
				public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
					System.out.println("start =============================");
					elements.forEach(e -> System.out.println(e));
					System.out.println("end =============================");
				}
			});
		env.execute("CountWithTimeoutTask");
	}
}

class BatchSendTrigger<T> extends Trigger<T, GlobalWindow> {

	private static final long serialVersionUID = 1L;

	// 最大缓存消息数量
	long maxCount;

	// 最大缓存时长
	long maxDelay;

	// 当前消息数量
	//int elementCount;

	// processing timer的时间
	long timerTime;
	
	private ValueStateDescriptor<Long> countValueStateDescriptor = new ValueStateDescriptor<>("count", LongSerializer.INSTANCE);

	public BatchSendTrigger(long maxCount, long maxDelay) {
		this.maxCount = maxCount;
		this.maxDelay = maxDelay;
	}

	public TriggerResult onElement(T element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
		ValueState<Long> countState = ctx.getPartitionedState(countValueStateDescriptor);
		Long elementCount = countState.value();
		
		if (elementCount == null) {
			elementCount = 0L;
			timerTime = timestamp + maxDelay;
			ctx.registerEventTimeTimer(timerTime);
		}

		countState.update(++elementCount);
		// maxCount条件满足
		if (elementCount >= maxCount) {
			countState.update(null);
			ctx.deleteEventTimeTimer(timerTime);
			return TriggerResult.FIRE_AND_PURGE;
		}

		return TriggerResult.CONTINUE;
	}

	public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
		// maxDelay条件满足
		ValueState<Long> countState = ctx.getPartitionedState(countValueStateDescriptor);
		countState.update(null);
		return TriggerResult.FIRE_AND_PURGE;
	}

	public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
		ValueState<Long> countState = ctx.getPartitionedState(countValueStateDescriptor);
		countState.update(null);
		return TriggerResult.FIRE_AND_PURGE;
	}

	public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
		ValueState<Long> countState = ctx.getPartitionedState(countValueStateDescriptor);
		countState.clear();
	}
}