package com.test.account;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountingInfoProcessWindowFunction extends ProcessWindowFunction<AccountingInfo, Tuple2<String, BigDecimal>, String, TimeWindow> {

	private static final long serialVersionUID = 7790420713942242032L;

	private static final Logger logger = LoggerFactory.getLogger(AccountingInfoProcessWindowFunction.class);

	private static final SimpleDateFormat fomart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void process(String key, ProcessWindowFunction<AccountingInfo, Tuple2<String, BigDecimal>, String, TimeWindow>.Context context,
			Iterable<AccountingInfo> input, Collector<Tuple2<String, BigDecimal>> collector) throws Exception {
		BigDecimal amount = BigDecimal.ZERO;
		for (AccountingInfo info : input) {
			amount = amount.add(new BigDecimal(info.getTransactionamount()));
			logger.info("key: {}, dataTime: {}, amount: {}", key, info.getTransactiondate(), info.getTransactionamount());
		}

		logger.info("key: {}, windowTime: [{}, {})", key, fomart.format(new Date(context.window().getStart())),
				fomart.format(new Date(context.window().getEnd())));
		collector.collect(new Tuple2<String, BigDecimal>(key, amount));
	}

}
