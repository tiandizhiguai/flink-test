package com.test.broadcast;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UserKeyedBroadcastProcess extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {

	private static final long serialVersionUID = -4635300979293211409L;

	private static final Logger logger = LoggerFactory.getLogger(UserKeyedBroadcastProcess.class);
	
	private MapStateDescriptor<String, Config> configStateDescriptor;
	
	private MapStateDescriptor<String, Map<String, UserEventContainer>> userStateDescriptor;

	public UserKeyedBroadcastProcess(MapStateDescriptor<String, Config> configStateDescriptor, 
			MapStateDescriptor<String, Map<String, UserEventContainer>> userStateDescriptor) {
		this.configStateDescriptor = configStateDescriptor;
		this.userStateDescriptor = userStateDescriptor;
	}

	@Override
	public void processBroadcastElement(Config value, Context context, Collector<EvaluatedResult> collector) throws Exception {
		context.getBroadcastState(configStateDescriptor).put(value.getChannel(), value);
	}

	@Override
	public void processElement(UserEvent value, ReadOnlyContext context, Collector<EvaluatedResult> out) throws Exception {
		String userId = value.getUserId();
		String channel = value.getChannel();
		
		MapState<String, Map<String, UserEventContainer>> userState = this.getRuntimeContext().getMapState(userStateDescriptor);
		Config config = context.getBroadcastState(configStateDescriptor).get(channel);
		
		if(Objects.isNull(config)) {
			logger.warn("config data is empty.");
			return;
		}
		
		Map<String, UserEventContainer> userEventContainerMap = userState.get(channel);
		if(Objects.isNull(userEventContainerMap)) {
			userEventContainerMap = Maps.newHashMap();
			userState.put(channel, userEventContainerMap);
		}
		
		UserEventContainer userEventContainer = userEventContainerMap.get(userId);
		if(Objects.isNull(userEventContainer)) {
			userEventContainer = new UserEventContainer();
			userEventContainer.setUserId(userId);
			userEventContainerMap.put(userId, userEventContainer);
		}
		userEventContainer.addUserEvent(value);
		
		if(EventType.PURCHASE.name().equals(value.getEventType())) {
			Optional<EvaluatedResult> result = compute(config, userEventContainer);
			result.ifPresent(r -> out.collect(r));
			userEventContainerMap.remove(userId);
		}
	}
	
	private Optional<EvaluatedResult> compute(Config config, UserEventContainer eventContainer) {
		Optional<EvaluatedResult> result = Optional.empty();
		String channel = config.getChannel();
		int historyPurchaseTimes = config.getHistoryPurchaseTimes();
		int maxPurchasePathLength = config.getMaxPurchasePathLength();
		
		int purchasePathLen = eventContainer.getUserEvents().size();
		if(historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
			
			final Map<String, Integer> eventSizeMap = Maps.newHashMap();
			eventContainer.getUserEvents()
				.stream()
				.sorted(Comparator.comparingLong(e -> e.getEventTimestamp()))
				.collect(Collectors.groupingBy(UserEvent::getEventType))
				.forEach((eventType, events) -> eventSizeMap.put(eventType, events.size()));
			
			EvaluatedResult evaluatedResult = new EvaluatedResult();
			evaluatedResult.setChannel(channel);
			evaluatedResult.setEventTypeCounts(eventSizeMap);
			evaluatedResult.setPurchasePathLen(purchasePathLen);
			evaluatedResult.setUserId(eventContainer.getUserId());
			result = Optional.of(evaluatedResult);
		}
		
		return result;
	}

}
