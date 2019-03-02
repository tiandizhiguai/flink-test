package com.test.async;

import java.util.Date;

public class Protocol {

	private long id;

	private String protocolNo;

	private int type;

	private String content;

	private String remark;

	private Date inputDate;

	private int isDelete;

	private Date stampDate;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getProtocolNo() {
		return protocolNo;
	}

	public void setProtocolNo(String protocolNo) {
		this.protocolNo = protocolNo;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public Date getInputDate() {
		return inputDate;
	}

	public void setInputDate(Date inputDate) {
		this.inputDate = inputDate;
	}

	public int getIsDelete() {
		return isDelete;
	}

	public void setIsDelete(int isDelete) {
		this.isDelete = isDelete;
	}

	public Date getStampDate() {
		return stampDate;
	}

	public void setStampDate(Date stampDate) {
		this.stampDate = stampDate;
	}

	@Override
	public String toString() {
		return "Protocol [id=" + id + ", protocolNo=" + protocolNo + ", type=" + type + ", content=" + content + ", remark=" + remark + ", inputDate="
				+ inputDate + ", isDelete=" + isDelete + ", stampDate=" + stampDate + "]";
	}
}
