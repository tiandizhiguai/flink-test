package com.test.account;

import java.math.BigDecimal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccuontingInfoWindow implements WindowFunction<AccountingInfo, Tuple2<String, BigDecimal>, String, TimeWindow> {

	private static final long serialVersionUID = -9163417231404559883L;

	private static final Logger logger = LoggerFactory.getLogger(RocketmqConsumer.class);
	
	@Override
	public void apply(String key, TimeWindow window, Iterable<AccountingInfo> input, Collector<Tuple2<String, BigDecimal>> out) throws Exception {
		BigDecimal amount = BigDecimal.ZERO;
		String accountNo = "";
		for (AccountingInfo info : input) {
			accountNo = info.getAccountnumber();
			amount = amount.add(new BigDecimal(info.getTransactionamount()));
			logger.info("window data: {}", info);
		}
		out.collect(new Tuple2<String, BigDecimal>(accountNo, amount));
	}

}
