package com.test.check;

import java.text.SimpleDateFormat;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BillInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TransactionInfo> {

	private static final long serialVersionUID = -5504853504737136873L;

	private static final Logger logger = LoggerFactory.getLogger(BillInfoTimestampExtractor.class);

	public BillInfoTimestampExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}	

	@Override
	public long extractTimestamp(TransactionInfo element) {
		try {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.getTransactionDate()).getTime();
		} catch (Exception e) {
			logger.error("failed to parse date, business no : " + element.getBusinessRecordNumber(), e);
			return 0;
		}
	}
	
}
