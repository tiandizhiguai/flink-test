package com.test.check;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FrontAccountDetailMap implements MapFunction<String, TransactionInfo> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper mapper = new ObjectMapper();

	@Override
	public TransactionInfo map(String value) throws Exception {
		TransactionInfo transactionInfo = mapper.readValue(value, TransactionInfo.class);
		transactionInfo.setSource("账务");
		return transactionInfo;
	}

}
