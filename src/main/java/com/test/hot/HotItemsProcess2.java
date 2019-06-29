package com.test.hot;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HotItemsProcess2 extends ProcessAllWindowFunction<UserBehavior, String, TimeWindow> {

	private static final long serialVersionUID = 1L;
	
	private static Map<String, ItemViewCount> itemViewCountMap = Collections.emptyMap();

	@Override
	public void process(Context context, Iterable<UserBehavior> inputs, Collector<String> collector) throws Exception {
		
		for(UserBehavior input : inputs) {
			ItemViewCount itemCount = itemViewCountMap.get(input.itemId);
			if (itemCount == null) {
				itemCount = new ItemViewCount();
				itemCount.itemId = input.itemId;
				itemCount.viewCount = 1;
				itemViewCountMap.put(input.itemId, itemCount);
			}else {
				itemCount.viewCount++;
			}
		}
		
		List<ItemViewCount> viewCountList = Collections.emptyList();
		viewCountList.addAll(itemViewCountMap.values());
		
		viewCountList.sort(new Comparator<ItemViewCount>() {

			@Override
			public int compare(ItemViewCount o1, ItemViewCount o2) {
				return (int) (o2.viewCount - o1.viewCount);
			}
		});
		
		StringBuilder result = new StringBuilder();
		result.append("====================================\n");
		result.append("时间: ").append(context.window().getEnd() - 1).append("\n");
		for (int i = 0; i < 3; i++) {
			ItemViewCount currentItem = viewCountList.get(i);
			// No1: 商品 ID=12224 浏览量=2413
			result.append("No").append(i).append(":").append(" 商品 ID=").append(currentItem.itemId).append(" 浏览量=").append(currentItem.viewCount)
					.append("\n");
		}
		result.append("====================================\n\n");
		
		collector.collect(result.toString());
	}

}
