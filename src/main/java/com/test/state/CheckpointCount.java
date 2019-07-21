package com.test.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

/**
 * 
 * @version V1.0
 * @Date 2019年6月26日 下午1:54:05
 * @since JDK 1.8
 */
public class CheckpointCount implements FlatMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>, CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private Long operatorCount;

	private ValueState<Long> keyedState;

	private ListState<Long> operatorState;

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		keyedState = context.getKeyedStateStore().getState(new ValueStateDescriptor<Long>("keyedState", TypeInformation.of(Long.class)));
		operatorState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("operatorState", TypeInformation.of(Long.class)));
		
		if(context.isRestored()) {
			Long count = 0L;
			Iterable<Long> operators = operatorState.get();
			for (Long operator : operators) {
				count += operator;
			}
			operatorCount = count;
		}else {
			operatorCount = 0L;
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		operatorState.clear();
		operatorState.add(operatorCount);
	}

	@Override
	public void flatMap(Tuple2<String, Long> value, Collector<Tuple3<String, Long, Long>> out) throws Exception {
		Long currentValue = keyedState.value();
		if(currentValue == null) {
			currentValue = 0L;
		}
		Long keyCount = currentValue + 1;
		keyedState.update(keyCount);
		operatorCount += 1;
		out.collect(Tuple3.of(value.f0, keyCount, operatorCount));
	}

}
