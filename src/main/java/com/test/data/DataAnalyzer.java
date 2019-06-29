package com.test.data;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class DataAnalyzer {

	private static final String[] fields = new String[] { "序号", "ID", "工单编号", "地市_x", "厂家_x", "网元类型", "基站名称", "小区号", "参数对象", "参数名称", "参数中文名称",
			"参数组ID", "参数值", "修改值", "参数等级", "是否影响业务", "交维状态", "覆盖类型_x", "实际覆盖场景", "覆盖场景", "最近修改次数", "操作类型", "网络制式_x", "工单状态", "当前操作人", "工单名称", "创建人",
			"网络制式_y", "清单详情", "创建时间", "工单备注", "工单来源", "截止修改时间", "工单等级", "创建人电话", "模板类型", "参数修改需求人", "参数修改需求人电话", "修改规模", "覆盖类型_y", "一级修改原因", "二级修改原因",
			"保障工单类型", };

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		CsvReader csvReader = env.readCsvFile("D:\\数据分析\\20181102\\data99.csv");
		csvReader.setCharset("GBK");
		csvReader.ignoreInvalidLines()
				.ignoreFirstLine()
				.pojoType(DataInfo.class, fields)
				.reduceGroup(new DataGroupReduce())
				.writeAsCsv("D:\\数据分析\\20181102\\data99_结果.csv", WriteMode.OVERWRITE)
				.setParallelism(1);
		
		env.execute("DataAnalyzer1102");
	}
}
