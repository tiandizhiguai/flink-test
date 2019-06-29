package com.test.hot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class HotItemsProcess extends KeyedProcessFunction<Long, ItemViewCount, String> {

	private static final long serialVersionUID = 1L;

	private final int topSize;

	private ListState<ItemViewCount> listState;

	public HotItemsProcess(int topSize) {
		this.topSize = topSize;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ListStateDescriptor<ItemViewCount> stateDescriptor = new ListStateDescriptor<>("stateDescriptor", ItemViewCount.class);
		listState = this.getRuntimeContext().getListState(stateDescriptor);
	}

	@Override
	public void processElement(ItemViewCount value, Context context, Collector<String> out) throws Exception {
		listState.add(value);
		context.timerService().registerEventTimeTimer(value.windowEnd + 1);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
		Iterable<ItemViewCount> allDatas = listState.get();
		List<ItemViewCount> resultDatas = new ArrayList<>();
		for (ItemViewCount d : allDatas) {
			resultDatas.add(d);
		}

		resultDatas.sort(new Comparator<ItemViewCount>() {

			@Override
			public int compare(ItemViewCount o1, ItemViewCount o2) {
				return (int) (o2.viewCount - o1.viewCount);
			}
		});

		StringBuilder result = new StringBuilder();
		result.append("====================================\n");
		result.append("时间: ").append(timestamp - 1).append("\n");
		for (int i = 0; i < topSize; i++) {
			ItemViewCount currentItem = resultDatas.get(i);
			// No1: 商品 ID=12224 浏览量=2413
			result.append("No").append(i).append(":").append(" 商品 ID=").append(currentItem.itemId).append(" 浏览量=").append(currentItem.viewCount)
					.append("\n");
		}
		result.append("====================================\n\n");
		
		out.collect(result.toString());
	}
}
