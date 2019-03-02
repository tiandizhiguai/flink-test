package com.test.async;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;

public class DataFunction implements Function<ResultSet, List<Protocol>> {

	@Override
	public List<Protocol> apply(ResultSet r) {
		List<JsonObject> results = r.getRows();
		return results.stream().map(e -> e.mapTo(Protocol.class)).collect(Collectors.toList());
	}

}
