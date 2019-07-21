package com.test.iteration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 
  *   功能说明：
 * https://mp.weixin.qq.com/s?__biz=MzA3MDY0NTMxOQ==&mid=2247486296&idx=1&sn=e946bb9a42559613711f6a381ee3215c&chksm=9f38e870a84f6166d319668f0fb0ec50ce0af1b87e920c26836d4e74079fec3ad27c8e5589ed&scene=21#wechat_redirect
  *   另一个例子：https://www.cnblogs.com/asker009/p/11103924.html
 * @version V1.0
 * @Date 2019年7月21日 下午9:47:56
 * @since JDK 1.8
 */
public class AggregatorWithoutParameterForIterateDelta {

	public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		List<Integer> data = new ArrayList<>();
		data.add(1);
		data.add(2);
		data.add(2);
		data.add(3);
		data.add(3);
		data.add(3);
		data.add(4);
		data.add(4);
		data.add(4);
		data.add(4);
		data.add(5);
		data.add(5);
		data.add(5);
		data.add(5);
		data.add(5);

		Collections.shuffle(data);

		DataSet<Tuple2<Integer, Integer>> initialSolutionSet = env.fromCollection(data).map(new TupleMakerMap());

		DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = initialSolutionSet.iterateDelta(initialSolutionSet, 20, 0);

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregator();
		iteration.registerAggregator("count.negative.elements", aggr);

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet()).where(0).equalTo(0).flatMap(new UpdateFilter());

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		List<Integer> result = null;
		try {
			result = iterationRes.map(new ProjectSecondMapper()).collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Collections.sort(result);
		for (int i : result) {
			System.out.println(i);
		}
	}

	private static final class TupleMakerMap extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private Random rnd;

		@Override
		public void open(Configuration parameters) {
			rnd = new Random(0xC0FFEBADBEEFDEADL + getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) {
			Integer nodeId = rnd.nextInt(100000);
			return new Tuple2<>(nodeId, value);
		}

	}

	private static final class AggregateMapDelta extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private LongSumAggregator aggr;

		private int superstep;

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator("count.negative.elements");
			superstep = getIterationRuntimeContext().getSuperstepNumber();
		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the elements that are equal to the superstep number
			if (value.f1 == superstep) {
				aggr.aggregate(1L);
			}
			return value;
		}
	}

	private static final class UpdateFilter
			extends RichFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private int superstep;

		@Override
		public void open(Configuration conf) {
			superstep = getIterationRuntimeContext().getSuperstepNumber();
		}

		@Override
		public void flatMap(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value, Collector<Tuple2<Integer, Integer>> out) {

			if (value.f0.f1 > superstep) {
				out.collect(value.f0);
			}
		}
	}

	private static final class ProjectSecondMapper extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f1;
		}
	}

}
