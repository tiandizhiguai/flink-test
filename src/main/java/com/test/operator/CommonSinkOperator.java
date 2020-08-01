package com.test.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * 
* 案例：假设我们现在需要实现一个通用的定时、定量的输出的StreamOperator。
实现步骤：
继承AbstractStreamOperator抽象类，实现OneInputStreamOperator接口
重写open方法，调用flink 提供的定时接口，并且注册定时器
重写initializeState/snapshotState方法，由于批量写需要做缓存，那么需要保证数据的一致性，将缓存数据存在状态中
重写processElement方法，将数据存在缓存中，达到一定大小然后输出
由于需要做定时调用，那么需要有一个定时调用的回调方法，那么定义的类需要实现ProcessingTimeCallback接口，并且实现其onProcessingTime方法。
* 
* @version V1.0
* @Date 2019年12月16日 下午3:54:09
* @since JDK 1.8
 */
public abstract class CommonSinkOperator<T extends Serializable> extends AbstractStreamOperator<Object>

		implements ProcessingTimeCallback, OneInputStreamOperator<T, Object> {

	private static final long serialVersionUID = 1L;

	private List<T> list;

	private ListState<T> listState;

	private int batchSize;

	private long interval;

	private ProcessingTimeService processingTimeService;
	
	private  Class<T> clazz;

	public CommonSinkOperator(Class<T> clazz) {
		this.clazz = clazz;
	}

	public CommonSinkOperator(int batchSize, long interval) {

		this.chainingStrategy = ChainingStrategy.ALWAYS;

		this.batchSize = batchSize;

		this.interval = interval;

	}

	@Override
	public void open() throws Exception {

		super.open();

		if (interval > 0 && batchSize > 1) {

			// 获取AbstractStreamOperator里面的ProcessingTimeService， 该对象用来做定时调用

			// 注册定时器将当前对象作为回调对象，需要实现ProcessingTimeCallback接口

			processingTimeService = getProcessingTimeService();

			long now = processingTimeService.getCurrentProcessingTime();

			processingTimeService.registerTimer(now + interval, this);

		}

	}

	// 状态恢复

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {

		super.initializeState(context);

		this.list = new ArrayList<T>();
		ListStateDescriptor<T> stateDescriptor = new ListStateDescriptor<T>("batch-interval-sink", clazz);
		listState = context.getOperatorStateStore().getListState(stateDescriptor);

		if (context.isRestored()) {

			listState.get().forEach(x -> {

				list.add(x);

			});

		}

	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {

		list.add(element.getValue());

		if (list.size() >= batchSize) {

			saveRecords(list);

		}

	}

	// checkpoint

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {

		super.snapshotState(context);

		if (list.size() > 0) {

			listState.clear();

			listState.addAll(list);

		}

	}

	// 定时回调
	@Override
	public void onProcessingTime(long timestamp) throws Exception {

		if (list.size() > 0) {

			saveRecords(list);

			list.clear();

		}

		long now = processingTimeService.getCurrentProcessingTime();

		processingTimeService.registerTimer(now + interval, this);// 再次注册

	}

	public abstract void saveRecords(List<T> datas);

}