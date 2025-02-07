/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ElasticsearchApiCallBridge} is used to bridge incompatible Elasticsearch Java API calls across different versions.
 * This includes calls to create Elasticsearch clients, handle failed item responses, etc. Any incompatible Elasticsearch
 * Java APIs should be bridged using this interface.
 *
 * <p>Implementations are allowed to be stateful. For example, for Elasticsearch 1.x, since connecting via an embedded node
 * is allowed, the call bridge will hold reference to the created embedded node. Each instance of the sink will hold
 * exactly one instance of the call bridge, and state cleanup is performed when the sink is closed.
 *
 * @param <C> The Elasticsearch client, that implements {@link AutoCloseable}.
 */
@Internal
public interface ElasticsearchApiCallBridge<C extends AutoCloseable> extends Serializable {

	/**
	 * Creates an Elasticsearch client implementing {@link AutoCloseable}.
	 *
	 * @param clientConfig The configuration to use when constructing the client.
	 * @return The created client.
	 */
	C createClient(Map<String, String> clientConfig) throws IOException;

	/**
	 * Creates a {@link BulkProcessor.Builder} for creating the bulk processor.
	 *
	 * @param client the Elasticsearch client.
	 * @param listener the bulk processor listender.
	 * @return the bulk processor builder.
	 */
	BulkProcessor.Builder createBulkProcessorBuilder(C client, BulkProcessor.Listener listener);

	/**
	 * Extracts the cause of failure of a bulk item action.
	 *
	 * @param bulkItemResponse the bulk item response to extract cause of failure
	 * @return the extracted {@link Throwable} from the response ({@code null} is the response is successful).
	 */
	@Nullable Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse);

	/**
	 * Set backoff-related configurations on the provided {@link BulkProcessor.Builder}.
	 * The builder will be later on used to instantiate the actual {@link BulkProcessor}.
	 *
	 * @param builder the {@link BulkProcessor.Builder} to configure.
	 * @param flushBackoffPolicy user-provided backoff retry settings ({@code null} if the user disabled backoff retries).
	 */
	void configureBulkProcessorBackoff(
      BulkProcessor.Builder builder,
      @Nullable BulkFlushBackoffPolicy flushBackoffPolicy);

	/**
	 * Creates a {@link RequestIndexer} that is able to work with {@link BulkProcessor} binary compatible.
	 */
	RequestIndexer createBulkProcessorIndexer(BulkProcessor bulkProcessor);

	/**
	 * Perform any necessary state cleanup.
	 */
	default void cleanup() {
		// nothing to cleanup by default
	}

	/**
	 * Used to control whether the retry delay should increase exponentially or remain constant.
	 */
	@PublicEvolving
	public enum FlushBackoffType {
		CONSTANT,
		EXPONENTIAL
	}

	/**
	 * Provides a backoff policy for bulk requests. Whenever a bulk request is rejected due to resource constraints
	 * (i.e. the client's internal thread pool is full), the backoff policy decides how long the bulk processor will
	 * wait before the operation is retried internally.
	 *
	 * <p>This is a proxy for version specific backoff policies.
	 */
	public static class BulkFlushBackoffPolicy implements Serializable {

		private static final long serialVersionUID = -6022851996101826049L;

		// the default values follow the Elasticsearch default settings for BulkProcessor
		private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
		private int maxRetryCount = 8;
		private long delayMillis = 50;

		public FlushBackoffType getBackoffType() {
			return backoffType;
		}

		public int getMaxRetryCount() {
			return maxRetryCount;
		}

		public long getDelayMillis() {
			return delayMillis;
		}

		public void setBackoffType(FlushBackoffType backoffType) {
			this.backoffType = checkNotNull(backoffType);
		}

		public void setMaxRetryCount(int maxRetryCount) {
			checkArgument(maxRetryCount >= 0);
			this.maxRetryCount = maxRetryCount;
		}

		public void setDelayMillis(long delayMillis) {
			checkArgument(delayMillis >= 0);
			this.delayMillis = delayMillis;
		}
	}

}
