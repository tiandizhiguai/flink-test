package com.test.sql;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class AccountingSqlAnalyzer {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "10.1.1.1:9092");
		props.setProperty("group.id", "accounting");
		
		FlinkKafkaConsumer<String> accountingConsumer = new FlinkKafkaConsumer<>("accounting", new SimpleStringSchema(), props);
		accountingConsumer.setStartFromEarliest();
		SingleOutputStreamOperator<AccountingInfo> acccountingDataSource = env.addSource(accountingConsumer)
				.map(new AccountingInfoMapFunction());
		Table acccountingTable = tenv.fromDataStream(acccountingDataSource);
		
		Table result = tenv.sqlQuery("select accountnumber, sum(transactionamount) as transactionamount from " + acccountingTable + " where transactiondate >= '2018-12-01 00:00:00' and transactiondate <= '2018-12-16 23:59:59' group by accountnumber");
		tenv.toRetractStream(result, AccountingResult.class)
			.print()
			.setParallelism(1);
		env.execute("AccountingSqlAnalyzer");
	}

}
