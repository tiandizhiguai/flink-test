package com.test.check;

import java.math.BigDecimal;

public class TransactionInfo {

	private String transactionType;

	private BigDecimal transactionAmount;

	private String businessRecordNumber;

	private String transactionDate;

	private String status;

	private String source;

	private String stampTime;

	private Integer failedCode;

	private String failedDesc;

	public String getFailedDesc() {
		return failedDesc;
	}

	public void setFailedDesc(String failedDesc) {
		this.failedDesc = failedDesc;
	}

	public Integer getFailedCode() {
		return failedCode;
	}

	public void setFailedCode(Integer failedCode) {
		this.failedCode = failedCode;
	}

	public String getStampTime() {
		return stampTime;
	}

	public void setStampTime(String stampTime) {
		this.stampTime = stampTime;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public BigDecimal getTransactionAmount() {
		return transactionAmount;
	}

	public void setTransactionAmount(BigDecimal transactionAmount) {
		this.transactionAmount = transactionAmount;
	}

	public String getBusinessRecordNumber() {
		return businessRecordNumber;
	}

	public void setBusinessRecordNumber(String businessRecordNumber) {
		this.businessRecordNumber = businessRecordNumber;
	}

	public String getTransactionDate() {
		return transactionDate;
	}

	public void setTransactionDate(String transactionDate) {
		this.transactionDate = transactionDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "TransactionInfo [transactionType=" + transactionType + ", transactionAmount=" + transactionAmount + ", businessRecordNumber="
				+ businessRecordNumber + ", transactionDate=" + transactionDate + ", status=" + status + ", source=" + source + ", stampTime="
				+ stampTime + "]";
	}

}
