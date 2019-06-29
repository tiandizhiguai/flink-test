package com.test.check;

public enum FailedTransactionEnum {
	
	CHANNEL_IN_PROCESS(4001, "渠道处理中"), 
	CHANNEL_FAILURE(4002, "渠道失败"),
	CHANNEL_SUCCESS_ACCOUNT_NONE(4003, "渠道成功，账务无记录"), 
	CHANNEL_DIFF_ACCOUNT(4004, "渠道和账务交易状态不一致"),
	BILL_IN_PROCESS(4005, "交易处理中"), 
	BILL_FAILURE(4006, "交易失败"),
	BILL_SUCCESS_ACCOUNT_NONE(4007, "交易成功，账务无记录"), 
	BILL_DIFF_ACCOUNT(4008, "交易和账务交易状态不一致"); 

	private int code;

	private String desc;

	private FailedTransactionEnum(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	public int getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}

}
