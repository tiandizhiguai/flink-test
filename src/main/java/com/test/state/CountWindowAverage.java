package com.test.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

	private static final long serialVersionUID = -3799369548002139075L;
	
	private ValueState<Tuple2<Long, Long>> sum;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(0)
        .flatMap(new CountWindowAverage())
        .print();
		
		env.execute("CountWindowAverage");
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
				"average",
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}));
		
		sum = this.getRuntimeContext().getState(descriptor);
	}

	@Override
	public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
		Tuple2<Long, Long> currentSum = sum.value();
		if (currentSum == null) {
			currentSum = Tuple2.of(0L, 0L);
		}
		
		currentSum.f0 += 1;
		currentSum.f1 += value.f1;
		sum.update(currentSum);
		
		if(currentSum.f0 > 2) {
			out.collect(Tuple2.of(value.f0, currentSum.f0 / currentSum.f1));
			sum.clear();
		}
	}

}
