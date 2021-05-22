package com.test.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class HudiTable {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "E:/software/hadoop-3.3.0");
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
		TableEnvironment env = TableEnvironment.create(settings);
		env.executeSql("CREATE TABLE t1(uuid VARCHAR(20),name VARCHAR(10),age INT,ts TIMESTAMP(3),`partition` VARCHAR(20)) PARTITIONED BY (`partition`) WITH ('connector' ='hudi','path' = 'e:/hudi','write.tasks' = '1', 'compaction.tasks' = '1', 'table.type' = 'COPY_ON_WRITE')");
		
		//插入一条数据
		env.executeSql("INSERT INTO t1 VALUES('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')")
			.print();
		env.sqlQuery("SELECT * FROM t1")//结果①
			.execute()
			.print();
		
		//修改数据
		env.executeSql("INSERT INTO t1 VALUES('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01','par1')")
			.print();
		env.sqlQuery("SELECT * FROM t1")//结果②
			.execute()
			.print();
	}

}
