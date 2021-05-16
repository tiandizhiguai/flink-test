package com.test.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, MySqlTwoPhaseCommitSink.ConnectionState, Void> {

	private static final long serialVersionUID = 1L;

	// 定义可用的构造函数
	public MySqlTwoPhaseCommitSink() {
		super(new KryoSerializer<>(MySqlTwoPhaseCommitSink.ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
	}

	@Override
	protected ConnectionState beginTransaction() throws Exception {
		System.out.println("=====> beginTransaction... ");
		// 使用连接池，不使用单个连接
		// Class.forName("com.mysql.jdbc.Driver");
		// Connection conn = DriverManager.getConnection("jdbc:mysql://172.16.200
		// .101:3306/bigdata?characterEncoding=UTF-8", "root", "123456");
		Connection connection = DruidConnectionPool.getConnection();
		connection.setAutoCommit(false);// 设定不自动提交
		return new ConnectionState(connection);
	}

	@Override
	protected void invoke(ConnectionState transaction, Tuple2<String, Integer> value, Context context) throws Exception {

		Connection connection = transaction.connection;
		PreparedStatement pstm = connection
				.prepareStatement("INSERT INTO t_wordcount (word, counts) VALUES (?, ?) ON" + " DUPLICATE KEY UPDATE counts = ?");
		pstm.setString(1, value.f0);
		pstm.setInt(2, value.f1);
		pstm.setInt(3, value.f1);
		pstm.executeUpdate();
		pstm.close();

	}

	// 先不做处理
	@Override
	protected void preCommit(ConnectionState transaction) throws Exception {
		System.out.println("=====> preCommit... " + transaction);
	}

	// 提交事务
	@Override
	protected void commit(ConnectionState transaction) {
		System.out.println("=====> commit... ");
		Connection connection = transaction.connection;
		try {
			connection.commit();
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException("提交事物异常");
		}
	}

	// 回滚事务
	@Override
	protected void abort(ConnectionState transaction) {
		System.out.println("=====> abort... ");
		Connection connection = transaction.connection;
		try {
			connection.rollback();
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException("回滚事物异常");
		}
	}

	// 定义建立数据库连接的方法
	public static class ConnectionState {

		private final transient Connection connection;

		public ConnectionState(Connection connection) {
			this.connection = connection;
		}
	}
}
