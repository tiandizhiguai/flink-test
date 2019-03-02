package com.test.async;

import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLConnection;

public class MySQLAsyncSink extends RichAsyncFunction<String, Protocol>{

	private static final long serialVersionUID = 1L;

	private AsyncSQLClient sqlClient;

	@Override
	public void open(Configuration parameters) throws Exception {
		JsonObject config = new JsonObject();
		config.put("host", "10.1.3.18").put("port", 8066).put("username", "admin").put("password", "test").put("database",
				"user");

		VertxOptions vo = new VertxOptions();
		vo.setEventLoopPoolSize(1);
		vo.setWorkerPoolSize(1);
		Vertx vertx = Vertx.vertx(vo);
		sqlClient = MySQLClient.createShared(vertx, config);
	}

	@Override
	public void asyncInvoke(String input, ResultFuture<Protocol> resultFuture) throws Exception {
		sqlClient.getConnection(res -> {
			if (res.failed()) {
				System.out.println("filed to get connection.");
				return;
			}

			SQLConnection connection = res.result();
			connection.query("select * from protocol", result -> {
				if (result.failed()) {
					System.out.println("filed to query.");
					return;
				}
				AsyncResult<List<Protocol>> dataResult = result.map(new DataFunction());
				resultFuture.complete(dataResult.result());
			});
		});
	}

}
