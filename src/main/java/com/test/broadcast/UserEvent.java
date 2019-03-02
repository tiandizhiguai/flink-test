package com.test.broadcast;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class UserEvent {

	private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

	private String userId;

	private String eventTime;

	private String channel;

	private String eventType;

	private Data data;

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public long getEventTimestamp() {
		try {
			return format.parse(eventTime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getEventTime() {
		return eventTime;
	}

	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}

	public static class Data {

		private long productId;

		private BigDecimal price;

		private BigDecimal amount;

		public long getProductId() {
			return productId;
		}

		public void setProductId(long productId) {
			this.productId = productId;
		}

		public BigDecimal getPrice() {
			return price;
		}

		public void setPrice(BigDecimal price) {
			this.price = price;
		}

		public BigDecimal getAmount() {
			return amount;
		}

		public void setAmount(BigDecimal amount) {
			this.amount = amount;
		}

	}
}
