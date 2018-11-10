package com.test.log;

import org.apache.flink.api.java.functions.KeySelector;

public class ClassKeySelector implements KeySelector<LogInfo, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String getKey(LogInfo value) throws Exception {
		return value.getLogMessage().getClazz();
	}

}
