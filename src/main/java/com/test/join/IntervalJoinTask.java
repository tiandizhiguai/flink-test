package com.test.join;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
* 用户购买商品过程中填写收货地址然后下单，在这个过程中产生两个数据流，
* 一个是订单数据流包含用户id、商品id、订单时间、订单金额、收货id等，
* 另一个是收货信息数据流包含收货id、收货人、收货人联系方式、收货人地址等，
* 系统在处理过程中，先发送订单数据，在之后的1到5秒内会发送收货数据，
* 现在要求实时统计按照不同区域维度的订单金额的top100地区。
* 
* @version V1.0
* @Date 2019年12月13日 下午3:11:36
* @since JDK 1.8
 */
public class IntervalJoinTask {

	private static final Logger logger = LoggerFactory.getLogger(IntervalJoinTask.class);
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(5000L);
		env.setParallelism(1);

		Order order = new Order("order01", "userId01", "gds01", 100D, "addrId01", 1573054200000L);
		KeyedStream<Order, String> orderStream = env.fromElements(order)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
					private static final long serialVersionUID = 1L;
					@Override
					public long extractTimestamp(Order element) {
						return element.getTime();
					}
				}).keyBy(e -> e.getAddrId());

		Address address = new Address("addrId01", "userId01", "beijing", 1573054203000L);
		Address address2 = new Address("addrId01", "userId01", "beijing", 1573054206000L);
		KeyedStream<Address, String> addressStream = env.fromElements(address, address2)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Address>(Time.seconds(10)) {
					private static final long serialVersionUID = 1L;
					@Override
					public long extractTimestamp(Address element) {
						return element.getTime();
					}
				}).keyBy(e -> e.getAddrId());

		orderStream.intervalJoin(addressStream)
			.between(Time.seconds(1), Time.seconds(5))
			.process(new ProcessJoinFunction<Order, Address, RsInfo>(){
				private static final long serialVersionUID = 1L;
				@Override
				public void processElement(Order order, Address address, Context context, Collector<RsInfo> ctx)
						throws Exception {
					logger.info("================================= key:{}, address:{}, amount:{}", 
							order.getAddrId(), address.getAddress(), order.getAmount());
				}
			});
		
		env.execute("IntervalJoinTask");
	}

	static class Order {

		private String orderId;

		private String userId;

		private String gdsId;

		private Double amount;

		private String addrId;

		private Long time;

		public Order(String orderId, String userId, String gdsId, Double amount, String addrId, Long time) {
			super();
			this.orderId = orderId;
			this.userId = userId;
			this.gdsId = gdsId;
			this.amount = amount;
			this.addrId = addrId;
			this.time = time;
		}

		public String getOrderId() {
			return orderId;
		}

		public void setOrderId(String orderId) {
			this.orderId = orderId;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public String getGdsId() {
			return gdsId;
		}

		public void setGdsId(String gdsId) {
			this.gdsId = gdsId;
		}

		public Double getAmount() {
			return amount;
		}

		public void setAmount(Double amount) {
			this.amount = amount;
		}

		public String getAddrId() {
			return addrId;
		}

		public void setAddrId(String addrId) {
			this.addrId = addrId;
		}

		public Long getTime() {
			return time;
		}

		public void setTime(Long time) {
			this.time = time;
		}

	}

	static class Address {

		private String addrId;

		private String userId;

		private String address;

		private Long time;

		public Address(String addrId, String userId, String address, Long time) {
			super();
			this.addrId = addrId;
			this.userId = userId;
			this.address = address;
			this.time = time;
		}

		public String getAddrId() {
			return addrId;
		}

		public void setAddrId(String addrId) {
			this.addrId = addrId;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

		public Long getTime() {
			return time;
		}

		public void setTime(Long time) {
			this.time = time;
		}

	}

	static class RsInfo {

		private String orderId;

		private String userId;

		private String gdsId;

		private Double amount;

		private String addrId;

		private String address;

		public RsInfo(String orderId, String userId, String gdsId, Double amount, String addrId, String address) {
			super();
			this.orderId = orderId;
			this.userId = userId;
			this.gdsId = gdsId;
			this.amount = amount;
			this.addrId = addrId;
			this.address = address;
		}

		public String getOrderId() {
			return orderId;
		}

		public void setOrderId(String orderId) {
			this.orderId = orderId;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public String getGdsId() {
			return gdsId;
		}

		public void setGdsId(String gdsId) {
			this.gdsId = gdsId;
		}

		public Double getAmount() {
			return amount;
		}

		public void setAmount(Double amount) {
			this.amount = amount;
		}

		public String getAddrId() {
			return addrId;
		}

		public void setAddrId(String addrId) {
			this.addrId = addrId;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

	}
}
