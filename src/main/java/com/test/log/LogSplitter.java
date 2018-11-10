package com.test.log;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class LogSplitter implements FlatMapFunction<String, LogInfo> {

	private static final long serialVersionUID = -354206349467319849L;

	@Override
	public void flatMap(String value, Collector<LogInfo> out) throws Exception {
		if (StringUtils.isNotBlank(value)) {
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			LogInfo logData = objectMapper.readValue(value, LogInfo.class);
			String messageStr = logData.getMessage();
			String[] messages = messageStr.split("\\s+");

			if (messages == null || messages.length < 6) {
				return;
			}

			LogMessage logMessage = new LogMessage();
			String className = messages[4];
			if (className.contains(":")) {
				logMessage.setClazz(className.split(":")[0]);
			} else {
				logMessage.setClazz(className);
			}
			logMessage.setDate(messages[0]);
			logMessage.setInfoLevel(messages[3]);
			logMessage.setInfo((messages[5]));
			logMessage.setThread(messages[2]);
			logMessage.setTime(messages[1]);
			logData.setLogMessage(logMessage);

			out.collect(logData);
		}
	}

}
