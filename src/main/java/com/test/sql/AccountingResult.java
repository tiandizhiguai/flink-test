package com.test.sql;

import java.io.Serializable;
import java.math.BigDecimal;

public class AccountingResult implements Serializable {

	private static final long serialVersionUID = 8849052200494487233L;

	private String accountnumber;

	private BigDecimal transactionamount;

	public String getAccountnumber() {
		return accountnumber;
	}

	public void setAccountnumber(String accountnumber) {
		this.accountnumber = accountnumber;
	}

	public BigDecimal getTransactionamount() {
		return transactionamount;
	}

	public void setTransactionamount(BigDecimal transactionamount) {
		this.transactionamount = transactionamount;
	}

	@Override
	public String toString() {
		return "AccountingResult [accountnumber=" + accountnumber + ", transactionamount=" + transactionamount + "]";
	}

}
