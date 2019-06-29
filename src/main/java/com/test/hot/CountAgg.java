package com.test.hot;

import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long createAccumulator() {
		return 0L;
	}

	@Override
	public Long add(UserBehavior value, Long accumulator) {
		return ++accumulator;
	}

	@Override
	public Long getResult(Long accumulator) {
		return accumulator;
	}

	@Override
	public Long merge(Long a, Long b) {
		return a + b;
	}

}
