package com.test.account;

import java.io.Serializable;

public class AccountingInfo implements Serializable {

	private static final long serialVersionUID = -2702000128604415851L;

	private String accountchangedetailid;

	private String partyid;

	private String accountnumber;

	private String transactionamount;

	private String transactiontype;

	private String transactionsite;

	private String businesstype;

	private String driction;

	private String accountdetailqueryid;

	private String businessrecordnumber;

	private String priorfreezeamount;

	private String prioruseableamount;

	private String transactiondate;

	private String priorctuamount;

	private String inputdate;

	private String description;

	private String frompartyid;

	private String fromaccountnumber;

	private String frompriorctuamount;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getFrompartyid() {
		return frompartyid;
	}

	public void setFrompartyid(String frompartyid) {
		this.frompartyid = frompartyid;
	}

	public String getFromaccountnumber() {
		return fromaccountnumber;
	}

	public void setFromaccountnumber(String fromaccountnumber) {
		this.fromaccountnumber = fromaccountnumber;
	}

	public String getTransactiondate() {
		return transactiondate;
	}

	public void setTransactiondate(String transactiondate) {
		this.transactiondate = transactiondate;
	}

	public String getAccountchangedetailid() {
		return accountchangedetailid;
	}

	public void setAccountchangedetailid(String accountchangedetailid) {
		this.accountchangedetailid = accountchangedetailid;
	}

	public String getPartyid() {
		return partyid;
	}

	public void setPartyid(String partyid) {
		this.partyid = partyid;
	}

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

	public String getTransactiontype() {
		return transactiontype;
	}

	public void setTransactiontype(String transactiontype) {
		this.transactiontype = transactiontype;
	}

	public String getBusinesstype() {
		return businesstype;
	}

	public void setBusinesstype(String businesstype) {
		this.businesstype = businesstype;
	}

	public String getDriction() {
		return driction;
	}

	public void setDriction(String driction) {
		this.driction = driction;
	}

	public String getAccountdetailqueryid() {
		return accountdetailqueryid;
	}

	public void setAccountdetailqueryid(String accountdetailqueryid) {
		this.accountdetailqueryid = accountdetailqueryid;
	}

	public String getBusinessrecordnumber() {
		return businessrecordnumber;
	}

	public void setBusinessrecordnumber(String businessrecordnumber) {
		this.businessrecordnumber = businessrecordnumber;
	}

	public String getPriorfreezeamount() {
		return priorfreezeamount;
	}

	public void setPriorfreezeamount(String priorfreezeamount) {
		this.priorfreezeamount = priorfreezeamount;
	}

	public String getPrioruseableamount() {
		return prioruseableamount;
	}

	public void setPrioruseableamount(String prioruseableamount) {
		this.prioruseableamount = prioruseableamount;
	}

	public String getPriorctuamount() {
		return priorctuamount;
	}

	public void setPriorctuamount(String priorctuamount) {
		this.priorctuamount = priorctuamount;
	}

	public String getInputdate() {
		return inputdate;
	}

	public void setInputdate(String inputdate) {
		this.inputdate = inputdate;
	}

	public String getFrompriorctuamount() {
		return frompriorctuamount;
	}

	public void setFrompriorctuamount(String frompriorctuamount) {
		this.frompriorctuamount = frompriorctuamount;
	}

	public String getTransactionsite() {
		return transactionsite;
	}

	public void setTransactionsite(String transactionsite) {
		this.transactionsite = transactionsite;
	}

	@Override
	public String toString() {
		return "AccountingInfo [accountchangedetailid=" + accountchangedetailid + ", partyid=" + partyid + ", accountnumber=" + accountnumber
				+ ", transactionamount=" + transactionamount + ", transactiontype=" + transactiontype + ", transactionsite=" + transactionsite
				+ ", businesstype=" + businesstype + ", driction=" + driction + ", accountdetailqueryid=" + accountdetailqueryid
				+ ", businessrecordnumber=" + businessrecordnumber + ", priorfreezeamount=" + priorfreezeamount + ", prioruseableamount="
				+ prioruseableamount + ", transactiondate=" + transactiondate + ", priorctuamount=" + priorctuamount + ", inputdate=" + inputdate
				+ ", description=" + description + ", frompartyid=" + frompartyid + ", fromaccountnumber=" + fromaccountnumber
				+ ", frompriorctuamount=" + frompriorctuamount + "]";
	}

}
