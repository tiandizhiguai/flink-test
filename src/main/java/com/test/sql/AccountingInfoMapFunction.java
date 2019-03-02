package com.test.sql;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountingInfoMapFunction implements MapFunction<String, AccountingInfo> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	public AccountingInfo map(String value) throws Exception {
		String[] values = value.split(",");
		if(values == null || values.length < 4) {
			return null;
		}
		AccountingInfo accountingInfo = new AccountingInfo();
		accountingInfo.setAccountnumber(values[2]);
		accountingInfo.setBusinessrecordnumber(values[8]);
		accountingInfo.setTransactionamount(new BigDecimal(values[3]));
		accountingInfo.setTransactiondate(values[11]);
		return accountingInfo;
	}

}
