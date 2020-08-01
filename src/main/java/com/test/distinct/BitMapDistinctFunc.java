package com.test.distinct;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * 
* 参考：
* https://mp.weixin.qq.com/s/4oC1W51AqLW4CJdPt7uUwQ
* 
* @version V1.0
* @Date 2020年1月17日 下午1:39:41
* @since JDK 1.8
 */
public class BitMapDistinctFunc implements AggregateFunction<Tuple3<Integer, String, Long>, Roaring64NavigableMap, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Roaring64NavigableMap createAccumulator() {
		return new Roaring64NavigableMap();
	}

	@Override
	public Roaring64NavigableMap add(Tuple3<Integer, String, Long> value, Roaring64NavigableMap accumulator) {
		//这里把需要统计的数据转化为对应的整型数，放入bitmap；
		accumulator.addLong(value.f1.hashCode());
		return accumulator;
	}

	@Override
	public Long getResult(Roaring64NavigableMap accumulator) {
		accumulator.runOptimize();
		return accumulator.getLongCardinality();
	}

	@Override
	public Roaring64NavigableMap merge(Roaring64NavigableMap a, Roaring64NavigableMap b) {
		a.or(b);
		return a;
	}
}