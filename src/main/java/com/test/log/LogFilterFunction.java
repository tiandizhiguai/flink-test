package com.test.log;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

public class LogFilterFunction implements FilterFunction<String> {

	private static final long serialVersionUID = 3676386189814156218L;

	private static final String DATE_TIME_PREFIX = "^[1-9]\\\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\\\s+(20|21|22|23|[0-1]\\\\d):[0-5]\\\\d:[0-5]\\\\d[\\\\s\\\\S]*$";

	@Override
	public boolean filter(String value) throws Exception {
		if (StringUtils.isBlank(value)) {
			return false;
		}
		Pattern p = Pattern.compile(DATE_TIME_PREFIX);
		return p.matcher(value).matches();
	}
}
