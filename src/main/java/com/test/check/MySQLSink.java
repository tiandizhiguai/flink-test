package com.test.check;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.IOUtils;

public class MySQLSink extends RichSinkFunction<List<TransactionInfo>> {

	private static final long serialVersionUID = 1L;

	private Connection connection;

	private PreparedStatement preparedStatement;

	@Override
	public void invoke(List<TransactionInfo> values, Context context) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		connection = DriverManager.getConnection("jdbc:mysql://10.7.13.48:8066/clearSettle?useUnicode=true&characterEncoding=utf-8", 
				"admin",
				"9MeRMf7b15SvsjLpQFtB");
		preparedStatement = connection.prepareStatement("INSERT INTO failed_transaction_detail(brn, transaction_type, transaction_date, failed_code, remark, gmt_created, gmt_modified) VALUES (?, ?, ?, ?, ?, now(), now())");
		
		for(TransactionInfo value : values) {
			preparedStatement.setString(1, value.getBusinessRecordNumber());
			preparedStatement.setString(2, value.getTransactionType());
			preparedStatement.setString(3, value.getTransactionDate());
			preparedStatement.setInt(4, value.getFailedCode());
			preparedStatement.setString(5, value.getFailedDesc());
		}
		
		preparedStatement.executeBatch();
		
		IOUtils.closeQuietly(preparedStatement);
		IOUtils.closeQuietly(connection);
	}
}
