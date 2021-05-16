package com.test.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

public class DruidConnectionPool {

	private transient static DataSource dataSource = null;

	private transient static Properties props = new Properties();

	// 静态代码块
	static {
		props.put("driverClassName", "com.mysql.jdbc.Driver");
		props.put("url", "jdbc:mysql://localhost:3306/day01?characterEncoding=utf8");
		props.put("username", "root");
		props.put("password", "123456");
		try {
			dataSource = null;// DruidDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private DruidConnectionPool() {
	}

	public static Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}
}
