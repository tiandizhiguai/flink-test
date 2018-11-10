package com.test.account;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class RocketmqConsumer extends RichParallelSourceFunction<String> {

	private static final Logger logger = LoggerFactory.getLogger(RocketmqConsumer.class);

	private static final long serialVersionUID = 1185282894107969541L;

	private static final String CONSUMER_GROUP_FIELD = "consumerGroup";

	private static final String NAMESRV_ADDR_FIELD = "namesrvAddr";

	private DefaultMQPushConsumer consumer;

	private Properties properties;

	private List<String> topics;

	private boolean isRunning = false;

	public RocketmqConsumer(List<String> topics, Properties properties) {
		this.properties = properties;
		this.topics = topics;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (consumer != null) {
			return;
		}
		consumer = new DefaultMQPushConsumer(properties.getProperty(CONSUMER_GROUP_FIELD));
		for (String topic : topics) {
			consumer.subscribe(topic, "*");
		}
		consumer.setNamesrvAddr(properties.getProperty(NAMESRV_ADDR_FIELD));
		consumer.setInstanceName(UUID.randomUUID().toString());
		consumer.setMessageModel(MessageModel.CLUSTERING);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.setConsumeThreadMax(128);
		consumer.setConsumeThreadMin(12);
		consumer.setConsumeMessageBatchMaxSize(1024);
		logger.info("create rocketmq consumer successfully.");
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		if (!isRunning) {
			consumer.registerMessageListener(new MessageListenerConcurrently() {

				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
					for (MessageExt msg : msgs) {
						try {
							String content = new String(msg.getBody(), "UTF-8");
							//logger.info("recieved msg content : {}", content);
							ctx.collect(content);
						} catch (IOException e) {
							logger.error("parse msg error.", e);
							return ConsumeConcurrentlyStatus.RECONSUME_LATER;
						}
					}
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}

			});
			try {
				consumer.start();
			} catch (MQClientException e) {
				logger.info("failed to start consumer, check it.", e);
			}
			isRunning = true;
		}
		while (isRunning) {
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		if (consumer != null) {
			consumer.shutdown();
		}
		isRunning = false;
	}

}
