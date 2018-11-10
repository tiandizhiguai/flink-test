package com.test.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class DataGroupReduceFunction implements GroupReduceFunction<DataInfo, Tuple3<String, String, String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<DataInfo> values, Collector<Tuple3<String, String, String>> out) throws Exception {
		groupByTwoFeilds(values, out);
	}

	private void groupByTwoFeilds(Iterable<DataInfo> values, Collector<Tuple3<String, String, String>> out) {

		Map<String, Set<String>> 保障工单类型s = new HashMap<>();

		for (DataInfo value : values) {
			Set<String> 参数值s = 保障工单类型s.get(value.get保障工单类型());
			if (参数值s == null) {
				参数值s = new HashSet<>();
				保障工单类型s.put(value.get保障工单类型(), 参数值s);
			}

			参数值s.add(value.get参数值());
		}

		for (Entry<String, Set<String>> 保障工单类型 : 保障工单类型s.entrySet()) {
			for (String 参数值 : 保障工单类型.getValue()) {
				out.collect(new Tuple3<String, String, String>(保障工单类型.getKey(), 参数值, "0"));
			}
		}

	}

	// private void groupByThreeFeilds(Iterable<DataInfo> values, Collector<Tuple3<String, String, String>> out) {
	//
	// Map<String, Map<String, Set<String>>> 保障工单类型s = new HashMap<>();
	//
	// for (DataInfo value : values) {
	// Map<String, Set<String>> 参数值s = 保障工单类型s.get(value.get保障工单类型());
	// if (参数值s == null) {
	// 参数值s = new HashMap<>();
	// 保障工单类型s.put(value.get保障工单类型(), 参数值s);
	// }
	//
	// Set<String> 修改值s = 参数值s.get(value.get参数值());
	// if (修改值s == null) {
	// 修改值s = new HashSet<>();
	// 参数值s.put(value.get参数值(), 修改值s);
	// }
	//
	// 修改值s.add(value.get修改值());
	// }
	//
	// for (Entry<String, Map<String, Set<String>>> 保障工单类型 : 保障工单类型s.entrySet()) {
	// for (Entry<String, Set<String>> 参数值 : 保障工单类型.getValue().entrySet()) {
	// for (String 修改值 : 参数值.getValue()) {
	// out.collect(new Tuple3<String, String, String>(保障工单类型.getKey(), 参数值.getKey(), 修改值));
	// }
	// }
	// }
	//
	// }

}
