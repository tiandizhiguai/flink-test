package com.test.account;

import java.io.Serializable;

public class AccountingInfo implements Serializable {

	private static final long serialVersionUID = -2702000128604415851L;

	private String accountnumber;

	private String transactionamount;

	private String businessrecordnumber;

	private String transactiondate;

	public String getAccountnumber() {
		return accountnumber;
	}

	public void setAccountnumber(String accountnumber) {
		this.accountnumber = accountnumber;
	}

	public String getTransactionamount() {
		return transactionamount;
	}

	public void setTransactionamount(String transactionamount) {
		this.transactionamount = transactionamount;
	}

	public String getBusinessrecordnumber() {
		return businessrecordnumber;
	}

	public void setBusinessrecordnumber(String businessrecordnumber) {
		this.businessrecordnumber = businessrecordnumber;
	}

	public String getTransactiondate() {
		return transactiondate;
	}

	public void setTransactiondate(String transactiondate) {
		this.transactiondate = transactiondate;
	}

	@Override
	public String toString() {
		return "AccountingInfo [accountnumber=" + accountnumber + ", transactionamount=" + transactionamount + ", businessrecordnumber="
				+ businessrecordnumber + ", transactiondate=" + transactiondate + "]";
	}

}
