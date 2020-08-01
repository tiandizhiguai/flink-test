package com.test.cep;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;

/**
 * 
* 实现功能：当15秒内两次监控温度超过阈值了发出预警，
* 当30秒内发生两次预警事件，并且第二次预警温度高于第一次预警温度则发出严重告警。
* 
* @version V1.0
* @Date 2020年3月30日 上午11:05:19
* @since JDK 1.8
 */
public class TemperatureMonitorTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> temperatureStream = env
                .addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        Pattern<JSONObject, JSONObject> alarmPattern = Pattern.<JSONObject>begin("alarm")
                .where(new SimpleCondition<JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getDouble("temperature") > 100.0d;
                    }
                })
                .times(2)
                .within(Time.seconds(15));

        DataStream<JSONObject> alarmStream = CEP.pattern(temperatureStream, alarmPattern)
                .select(new PatternSelectFunction<JSONObject, JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("alarm").stream()
                                .max(Comparator.comparingDouble(o -> o.getLongValue("temperature")))
                                .orElseThrow(() -> new IllegalStateException("should contains 2 events, but none"));
                    }
                }).setParallelism(1);

        Pattern<JSONObject, JSONObject> criticalPattern = Pattern.<JSONObject>begin("critical")
                .times(2)
                .within(Time.seconds(30));

        DataStream<JSONObject> criticalStream = CEP.pattern(alarmStream, criticalPattern)
                .flatSelect(new PatternFlatSelectFunction<JSONObject, JSONObject>() {
					private static final long serialVersionUID = 1L;

					@Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern,
                                           Collector<JSONObject> out) throws Exception {
                        List<JSONObject> critical = pattern.get("critical");
                        JSONObject first = critical.get(0);
                        JSONObject second = critical.get(1);
                        if (first.getLongValue("temperature") <
                                second.getLongValue("temperature")) {
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.putAll(second);
                            out.collect(jsonObject);
                        }
                    }
                }).setParallelism(1);
        criticalStream.print().setParallelism(1);
        env.execute();
    }

    private static class PeriodicSourceFunction extends RichSourceFunction<JSONObject> {

		private static final long serialVersionUID = 1L;
		
		private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicSourceFunction() {
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                ctx.collect(buildEvent());
            }
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            double temperature = RandomUtils.nextDouble(0, 200);
            ret.put("id", id.getAndIncrement());
            ret.put("timestamp", System.currentTimeMillis());
            ret.put("temperature", temperature);
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }

    private static class EventTimestampPeriodicWatermarks implements AssignerWithPeriodicWatermarks<JSONObject> {
		private static final long serialVersionUID = 1L;
		
		private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
            Long timestamp = element.getLong("timestamp");
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }
    }
}
