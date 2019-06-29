package com.test.hot;

public class ItemViewCount {

	public String itemId; // 商品 ID

	public long windowEnd; // 窗口结束时间戳

	public long viewCount; // 商品的点击量

	public static ItemViewCount of(String itemId, long windowEnd, long viewCount) {
		ItemViewCount result = new ItemViewCount();
		result.itemId = itemId;
		result.windowEnd = windowEnd;
		result.viewCount = viewCount;
		return result;
	}
}