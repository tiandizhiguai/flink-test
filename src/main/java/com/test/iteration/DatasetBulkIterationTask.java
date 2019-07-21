package com.test.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetBulkIterationTask {

	private static final Logger logger = LoggerFactory.getLogger(DatasetBulkIterationTask.class);
	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		IterativeDataSet<Integer> initial = env.fromElements(1, 2).iterate(10);
		DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Integer map(Integer value) throws Exception {
				Integer result = value + 1;
				logger.info("==================value : {}, result : {}", value, result);
				return result;
			}
		});
		
		initial.closeWith(iteration).print();
	}
}
