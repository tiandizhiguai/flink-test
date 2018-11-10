package com.test.account;

import org.apache.flink.api.java.functions.KeySelector;

public class AccountingInfoKeySelector implements KeySelector<AccountingInfo, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String getKey(AccountingInfo value) throws Exception {
		return value.getAccountnumber();
	}

}
