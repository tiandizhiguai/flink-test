package com.test.account;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountingInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<AccountingInfo> {

	private static final long serialVersionUID = -2178275482735116111L;

	private static final Logger logger = LoggerFactory.getLogger(AccountingInfoTimestampExtractor.class);

	private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public AccountingInfoTimestampExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}

	@Override
	public long extractTimestamp(AccountingInfo element) {
		try {
			return format.parse(element.getTransactiondate()).getTime();
		} catch (ParseException e) {
			logger.error("failed to parse date, business no : " + element.getBusinessrecordnumber(), e);
			return 0;
		}
	}
}
