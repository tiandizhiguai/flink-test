package com.test.trigger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

/**
 * 定时输出：
* 求每个区域的每小时的商品销售额, 要求每隔1min能能够看到销售额变动情况
* 
* @version V1.0
* @Date 2019年12月3日 下午6:43:45
* @since JDK 1.8
 */
public class ContinuousEventTimeTriggerTask {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(5000);
		env.setParallelism(1);

		env.fromElements(new Order("orderId03", 1573874530000L, "gdsId03", 300D, "beijing"),
				new Order("orderId03", 1573874740000L, "gdsId03", 300D, "hangzhou"))
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(30)) {
					private static final long serialVersionUID = 1L;
					@Override
					public long extractTimestamp(Order element) {
						return element.getOrderTime();
					}
				})
				.map(new MapFunction<Order, Tuple2<String, Double>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Double> map(Order value) throws Exception {
						return new Tuple2<String, Double>(value.getAreaId(), value.getAmount());
					}
				})
				.keyBy(0)
				.timeWindow(Time.hours(1))
				.trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
				.reduce((v1, v2) -> new Tuple2<String, Double>(v1.f0, v1.f1 + v2.f1))
				.print();

		env.execute("ContinuousEventTimeTriggerTest");
	}

	static class Order {

		private String orderId;

		private Long orderTime;

		private String gdsId;

		private Double amount;

		private String areaId;

		public Order(String orderId, Long orderTime, String gdsId, Double amount, String areaId) {
			this.orderId = orderId;
			this.orderTime = orderTime;
			this.gdsId = gdsId;
			this.amount = amount;
			this.areaId = areaId;
		}

		public String getOrderId() {
			return orderId;
		}

		public void setOrderId(String orderId) {
			this.orderId = orderId;
		}

		public Long getOrderTime() {
			return orderTime;
		}

		public void setOrderTime(Long orderTime) {
			this.orderTime = orderTime;
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

		public String getAreaId() {
			return areaId;
		}

		public void setAreaId(String areaId) {
			this.areaId = areaId;
		}

	}
}
