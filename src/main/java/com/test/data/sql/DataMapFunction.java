package com.test.data.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DataMapFunction implements MapFunction<DataSqlResult, Tuple2<String, String>> {

	private static final long serialVersionUID = -7851913646200420608L;

	@Override
	public Tuple2<String, String> map(DataSqlResult value) throws Exception {
		return Tuple2.of(value.get保障工单类型(), value.get参数值());
	}

}
