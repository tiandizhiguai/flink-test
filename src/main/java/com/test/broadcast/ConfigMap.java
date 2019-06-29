package com.test.broadcast;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigMap implements MapFunction<String, Config> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper mapper = new ObjectMapper();

	@Override
	public Config map(String value) throws Exception {
		return mapper.readValue(value, Config.class);
	}

}
