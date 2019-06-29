/**
 * Copyright © 2014-2019 TransfarPay.All Rights Reserved.
 */
package com.test.hot;

/** 用户行为数据结构 **/
public class UserBehavior {

	public long userId; // 用户 ID

	public String itemId; // 商品 ID

	public int categoryId; // 商品类目 ID

	public String behavior; // 用户行为, 包括("pv", "buy", "cart", "fav")

	public long timestamp; // 行为发生的时间戳，单位秒

	@Override
	public String toString() {
		return "UserBehavior [userId=" + userId + ", itemId=" + itemId + ", categoryId=" + categoryId + ", behavior=" + behavior + ", timestamp="
				+ timestamp + "]";
	}

}