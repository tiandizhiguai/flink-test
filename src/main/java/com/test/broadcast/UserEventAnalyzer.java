package com.test.broadcast;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 
* 我们把输入的用户操作行为事件，实时存储到Kafka的一个Topic中，
* 对于相关的配置也使用一个Kafka Topic来存储，这样就会构建了
* 2个Stream：一个是普通的Stream，用来处理用户行为事件；
* 另一个是Broadcast Stream，用来处理并更新配置信息。计算得
* 到的最终结果，会保存到另一个Kafka的Topic中，供外部其他系统
* 消费处理以支撑运营或分析活动。
* 
* @version V1.0
* @Date 2019年1月23日 下午2:30:21
* @since JDK 1.8
 */
public class UserEventAnalyzer {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new MemoryStateBackend());
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
		checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
		checkpointConfig.setCheckpointInterval(5 * 60 * 1000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "10.7.30.22:9092");
		props.setProperty("group.id", "user");

		FlinkKafkaConsumer<String> kafkaUserEventSource = new FlinkKafkaConsumer<>("input-event-topic", new SimpleStringSchema(), props);
		kafkaUserEventSource.setStartFromEarliest();
		KeyedStream<UserEvent, String> customerUserEventStream = env.addSource(kafkaUserEventSource)
			.map(new UserMap())
			.assignTimestampsAndWatermarks(new UserWatermarkExtractor(Time.days(1000)))
			.keyBy(new UserKey());
		
		FlinkKafkaConsumer<String> kafkaConfigEventSource = new FlinkKafkaConsumer<>("input-config-topic", new SimpleStringSchema(), props);
		kafkaConfigEventSource.setStartFromEarliest();
		MapStateDescriptor<String, Config> configStateDescriptor = new MapStateDescriptor<>("ConfigBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,
				TypeInformation.of(new TypeHint<Config>() {}));
		MapStateDescriptor<String, Map<String, UserEventContainer>> userStateDescriptor = new MapStateDescriptor<>("UserState",BasicTypeInfo.STRING_TYPE_INFO,
				new MapTypeInfo<>(String.class, UserEventContainer.class));
		
		BroadcastStream<Config> configBroadcastStream = env.addSource(kafkaConfigEventSource)
			.map(new ConfigMap())
			.broadcast(configStateDescriptor);
		
		customerUserEventStream.connect(configBroadcastStream)
			.process(new UserKeyedBroadcastProcess(configStateDescriptor, userStateDescriptor))
			.print();
		
		env.execute("UserEventAnalyzer");
	}
}
