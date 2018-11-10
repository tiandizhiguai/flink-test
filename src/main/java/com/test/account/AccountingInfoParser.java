package com.test.account;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountingInfoParser implements FlatMapFunction<String, AccountingInfo> {

	private static final long serialVersionUID = 1L;
	
	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	public void flatMap(String value, Collector<AccountingInfo> out) throws Exception {
		AccountingInfo accountingInfo = mapper.readValue(value, AccountingInfo.class);
		String transactiondate = accountingInfo.getTransactiondate();
		if(transactiondate.contains(".")) {
			accountingInfo.setTransactiondate(transactiondate.substring(0, transactiondate.indexOf(".")));
		}
		out.collect(accountingInfo);
	}

}
