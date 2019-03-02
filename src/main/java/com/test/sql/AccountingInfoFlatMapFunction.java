package com.test.sql;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountingInfoFlatMapFunction implements FlatMapFunction<String, AccountingInfo> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	public void flatMap(String value, Collector<AccountingInfo> out) throws Exception {
		String[] values = value.split(",");
		AccountingInfo accountingInfo = new AccountingInfo();
		accountingInfo.setAccountnumber(values[2]);
		accountingInfo.setBusinessrecordnumber(values[8]);
		accountingInfo.setTransactionamount(new BigDecimal(values[3]));
		accountingInfo.setTransactiondate(values[11]);
		out.collect(accountingInfo);
	}

}
