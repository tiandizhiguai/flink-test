package com.test.state;

import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 
* 计算每个分组的最小值，该程序未调试正确。
* 
* @version V1.0
* @Date 2019年6月26日 下午1:54:05
* @since JDK 1.8
 */
public class LeastValueTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(2, 2L), Tuple2.of(2, 1L), Tuple2.of(5, 1L), Tuple2.of(5, 2L), Tuple2.of(5, 3L))
			.keyBy(e -> e.f0)
			.flatMap(new RichFlatMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {

				private static final long serialVersionUID = 1L;
				
				private MapState<Integer, Long> mapState;

				@Override
				public void open(Configuration parameters) throws Exception {
					MapStateDescriptor<Integer, Long> descriptor = new MapStateDescriptor<>(
							"mapState",
							TypeInformation.of(new TypeHint<Integer>(){}),
							TypeInformation.of(new TypeHint<Long>(){}));
					mapState = this.getRuntimeContext().getMapState(descriptor);
				}

				@Override
				public void flatMap(Tuple2<Integer, Long> value, Collector<Tuple2<Integer, Long>> out) throws Exception {
					Long leastValue = mapState.get(value.f0);
					if(leastValue == null || value.f1 < leastValue) {
						mapState.put(value.f0, value.f1);
					}else {
						for (Map.Entry<Integer, Long> entry : mapState.entries()) {
							out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
						}
					}
				}})
			.print();
		env.execute("LeastValueTask");
	}
}
