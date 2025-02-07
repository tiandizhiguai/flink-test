/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.test.es;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.InstantiationUtil;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.es.ElasticsearchApiCallBridge.BulkFlushBackoffPolicy;
import com.test.es.ElasticsearchApiCallBridge.FlushBackoffType;

/**
 * Base class for all Flink Elasticsearch Sinks.
 *
 * <p>This class implements the common behaviour across Elasticsearch versions, such as
 * the use of an internal {@link BulkProcessor} to buffer multiple {@link ActionRequest}s before
 * sending the requests to the cluster, as well as passing input records to the user provided
 * {@link ElasticsearchSinkFunction} for processing.
 *
 * <p>The version specific API calls for different Elasticsearch versions should be defined by a concrete implementation of
 * a {@link ElasticsearchApiCallBridge}, which is provided to the constructor of this class. This call bridge is used,
 * for example, to create a Elasticsearch {@link Client}, handle failed item responses, etc.
 *
 * @param <T> Type of the elements handled by this sink
 * @param <C> Type of the Elasticsearch client, which implements {@link AutoCloseable}
 */
@Internal
public abstract class AbstractElasticsearchOutputFormat<T, C extends AutoCloseable> extends
		RichOutputFormat<T> {

	private static final long serialVersionUID = -1007596293618451942L;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractElasticsearchOutputFormat.class);

	// ------------------------------------------------------------------------
	//  Internal bulk processor configuration
	// ------------------------------------------------------------------------

	public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
	public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE = "bulk.flush.backoff.enable";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE = "bulk.flush.backoff.type";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES = "bulk.flush.backoff.retries";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY = "bulk.flush.backoff.delay";

	private final Integer bulkProcessorFlushMaxActions;
	private final Integer bulkProcessorFlushMaxSizeMb;
	private final Long bulkProcessorFlushIntervalMillis;
	private final BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

	// ------------------------------------------------------------------------
	//  User-facing API and configuration
	// ------------------------------------------------------------------------

	/**
	 * The config map that contains configuration for the bulk flushing behaviours.
	 *
	 * <p>For {@link org.elasticsearch.client.transport.TransportClient} based implementations, this config
	 * map would also contain Elasticsearch-shipped configuration, and therefore this config map
	 * would also be forwarded when creating the Elasticsearch client.
	 */
	private final Map<String, String> userConfig;

	/** The function that is used to construct multiple {@link DocWriteRequest DocWriteRequests} from each incoming element. */
	private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

	/** User-provided handler for failed {@link DocWriteRequest DocWriteRequests}. */
	private final DocWriteRequestFailureHandler failureHandler;

	/** Provided to the user via the {@link ElasticsearchSinkFunction} to add {@link DocWriteRequest DocWriteRequests}. */
	private transient RequestIndexer requestIndexer;

	// ------------------------------------------------------------------------
	//  Internals for the Flink Elasticsearch Sink
	// ------------------------------------------------------------------------

	/** Call bridge for different version-specific. */
	private final ElasticsearchApiCallBridge<C> callBridge;

	/** Elasticsearch client created using the call bridge. */
	private transient C client;

	/** Bulk processor to buffer and send requests to Elasticsearch, created using the client. */
	private transient BulkProcessor bulkProcessor;

	/**
	 * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown in callbacks and
	 * the user considered it should fail the sink via the
	 * {@link DocWriteRequestFailureHandler#onFailure(DocWriteRequest, Throwable, int, RequestIndexer)} method.
	 *
	 * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public AbstractElasticsearchOutputFormat(
		ElasticsearchApiCallBridge callBridge,
		Map<String, String> userConfig,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		DocWriteRequestFailureHandler failureHandler) {

		this.callBridge = checkNotNull(callBridge);
		this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);
		this.failureHandler = checkNotNull(failureHandler);

		// we eagerly check if the user-provided sink function and failure handler is serializable;
		// otherwise, if they aren't serializable, users will merely get a non-informative error message
		// "ElasticsearchSinkBase is not serializable"

		checkArgument(InstantiationUtil.isSerializable(elasticsearchSinkFunction),
			"The implementation of the provided ElasticsearchSinkFunction is not serializable. " +
				"The object probably contains or references non-serializable fields.");

		checkArgument(InstantiationUtil.isSerializable(failureHandler),
			"The implementation of the provided DocWriteRequestFailureHandler is not serializable. " +
				"The object probably contains or references non-serializable fields.");

		// extract and remove bulk processor related configuration from the user-provided config,
		// so that the resulting user config only contains configuration related to the Elasticsearch client.

		checkNotNull(userConfig);

		// copy config so we can remove entries without side-effects
		userConfig = new HashMap<>(userConfig);

		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorFlushMaxActions = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
		} else {
			bulkProcessorFlushMaxActions = null;
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorFlushMaxSizeMb = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
		} else {
			bulkProcessorFlushMaxSizeMb = null;
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorFlushIntervalMillis = params.getLong(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
		} else {
			bulkProcessorFlushIntervalMillis = null;
		}

		boolean bulkProcessorFlushBackoffEnable = params.getBoolean(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
		userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

		if (bulkProcessorFlushBackoffEnable) {
			this.bulkProcessorFlushBackoffPolicy = new BulkFlushBackoffPolicy();

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
				bulkProcessorFlushBackoffPolicy.setBackoffType(FlushBackoffType.valueOf(params.get(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
			}

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
				bulkProcessorFlushBackoffPolicy.setMaxRetryCount(params.getInt(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
			}

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
				bulkProcessorFlushBackoffPolicy.setDelayMillis(params.getLong(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
			}

		} else {
			bulkProcessorFlushBackoffPolicy = null;
		}

		this.userConfig = userConfig;
	}

	@Override
	public void open(int taskNumber, int numberTasks) throws IOException {
		client = callBridge.createClient(userConfig);
		bulkProcessor = buildBulkProcessor(new BulkProcessorListener());
		requestIndexer = callBridge.createBulkProcessorIndexer(bulkProcessor);
	}

	@Override
	public void writeRecord(T value) throws IOException {
		// if bulk processor callbacks have previously reported an error, we rethrow the error and fail the sink
		checkErrorAndRethrow();
		elasticsearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
	}

	@Override
	public void close() throws IOException {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			try {
				client.close();
			} catch (Exception e) {
				throw new RuntimeException("Fail to close client: " + client, e);
			}
			client = null;
		}

		callBridge.cleanup();

		// make sure any errors from callbacks are rethrown
		checkErrorAndRethrow();
	}

	/**
	 * Build the {@link BulkProcessor}.
	 *
	 * <p>Note: this is exposed for testing purposes.
	 */
	@VisibleForTesting
	protected BulkProcessor buildBulkProcessor(BulkProcessor.Listener listener) {
		checkNotNull(listener);

		BulkProcessor.Builder bulkProcessorBuilder = callBridge.createBulkProcessorBuilder(client, listener);

		// This makes flush() blocking
		bulkProcessorBuilder.setConcurrentRequests(0);

		if (bulkProcessorFlushMaxActions != null) {
			bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
		}

		if (bulkProcessorFlushMaxSizeMb != null) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeMb, ByteSizeUnit.MB));
		}

		if (bulkProcessorFlushIntervalMillis != null) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
		}

		// if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
		callBridge.configureBulkProcessorBackoff(bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

		return bulkProcessorBuilder.build();
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
		}
	}

	private class BulkProcessorListener implements BulkProcessor.Listener {
		@Override
		public void beforeBulk(long executionId, BulkRequest request) { }

		@Override
		public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			if (response.hasFailures()) {
				BulkItemResponse itemResponse;
				Throwable failure;
				RestStatus restStatus;

				try {
					for (int i = 0; i < response.getItems().length; i++) {
						itemResponse = response.getItems()[i];
						failure = callBridge.extractFailureCauseFromBulkItemResponse(itemResponse);
						if (failure != null) {
							LOG.error("Failed Elasticsearch item request: {}", itemResponse.getFailureMessage(), failure);

							restStatus = itemResponse.getFailure().getStatus();
							if (restStatus == null) {
								failureHandler.onFailure(request.requests().get(i), failure, -1, requestIndexer);
							} else {
								failureHandler.onFailure(request.requests().get(i), failure, restStatus.getStatus(), requestIndexer);
							}
						}
					}
				} catch (Throwable t) {
					// fail the sink and skip the rest of the items
					// if the failure handler decides to throw an exception
					failureThrowable.compareAndSet(null, t);
				}
			}
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			LOG.error("Failed Elasticsearch bulk request: {}", failure.getMessage(), failure.getCause());

			try {
				for (DocWriteRequest action : request.requests()) {
					failureHandler.onFailure(action, failure, -1, requestIndexer);
				}
			} catch (Throwable t) {
				// fail the sink and skip the rest of the items
				// if the failure handler decides to throw an exception
				failureThrowable.compareAndSet(null, t);
			}
		}
	}

}
