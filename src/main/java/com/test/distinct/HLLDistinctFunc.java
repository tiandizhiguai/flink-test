package com.test.distinct;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class HLLDistinctFunc implements AggregateFunction<Tuple3<Integer, String, Long>, HyperLogLog, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public HyperLogLog createAccumulator() {
		return new HyperLogLog(0.01);
	}

	@Override
	public HyperLogLog add(Tuple3<Integer, String, Long> value, HyperLogLog accumulator) {
		accumulator.offer(value.f1);
		return accumulator;
	}

	@Override
	public Long getResult(HyperLogLog accumulator) {
		return accumulator.cardinality();
	}

	@Override
	public HyperLogLog merge(HyperLogLog a, HyperLogLog b) {
		try {
			a.addAll(b);
		} catch (CardinalityMergeException e) {
			e.printStackTrace();
		}
		return a;
	}
}
