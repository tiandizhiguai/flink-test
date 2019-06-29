package com.test.check;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeftCoGroup implements CoGroupFunction<TransactionInfo, 
	TransactionInfo, List<TransactionInfo>> {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(LeftCoGroup.class);

	@Override
	public void coGroup(Iterable<TransactionInfo> bill, Iterable<TransactionInfo> frontAccount, 
			Collector<List<TransactionInfo>> collector) throws Exception {
		logger.info(">>>>>>>>>>>> start to check data.");
		
		Iterator<TransactionInfo> billIterator = bill.iterator();
		Iterator<TransactionInfo> frontAccountIterator = frontAccount.iterator();
		List<TransactionInfo> failedDatas = Collections.emptyList();
		
		//1.渠道多余的交易
		if (billIterator.hasNext() && !frontAccountIterator.hasNext()) {
			TransactionInfo lastElement = getLastElement(billIterator);
			TransactionInfo result = getResultInfo(lastElement);
			switch(lastElement.getStatus()) {
				case "已退款":
					break;
				case "成功":
					collect(failedDatas,result, FailedTransactionEnum.BILL_SUCCESS_ACCOUNT_NONE);
				break;
				case "处理中":
					collect(failedDatas,result, FailedTransactionEnum.BILL_IN_PROCESS);
				break;
				case "失败":
					collect(failedDatas,result, FailedTransactionEnum.BILL_FAILURE);
				break;
			}
		//2.不一致的交易
		}else if (!billIterator.hasNext() && !frontAccountIterator.hasNext()) {
			TransactionInfo billInfo = getLastElement(billIterator);
			TransactionInfo frontAccountInfo = getLastElement(frontAccountIterator);
			TransactionInfo result = getResultInfo(billInfo);
			if(!billInfo.getStatus().equals(frontAccountInfo.getStatus())) {
				collect(failedDatas,result, FailedTransactionEnum.BILL_DIFF_ACCOUNT);
			}
		}
		
		collector.collect(failedDatas);
		
		logger.info(">>>>>>>>>>>> end to check data.");
	}
	
	private void collect(List<TransactionInfo> failedDatas, TransactionInfo result, 
			FailedTransactionEnum failedTransactionType) {
		result.setFailedCode(failedTransactionType.getCode());
		result.setFailedDesc(failedTransactionType.getDesc());
		failedDatas.add(result);
	}
	
	private TransactionInfo getResultInfo(TransactionInfo transactionInfo) {
		TransactionInfo result = new TransactionInfo();
		result.setBusinessRecordNumber(transactionInfo.getBusinessRecordNumber());
		result.setSource(transactionInfo.getSource());
		result.setTransactionDate(transactionInfo.getTransactionDate());
		result.setTransactionType(transactionInfo.getTransactionType());
		
		return result;
	}
	
	private TransactionInfo getLastElement(Iterator<TransactionInfo> iterator) {
		TransactionInfo lastElement = null;
		for (; iterator.hasNext();) {
			lastElement = iterator.next();
		}
		
		return lastElement;
	}

}
