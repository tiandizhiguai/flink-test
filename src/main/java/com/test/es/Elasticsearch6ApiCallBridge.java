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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 6 and later versions.
 */
@Internal
public class Elasticsearch6ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6ApiCallBridge.class);

	/**
	 * User-provided HTTP Host.
	 */
	private final List<HttpHost> httpHosts;

	/**
	 * The factory to configure the rest client.
	 */
	private final RestClientFactory restClientFactory;

	Elasticsearch6ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
		Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());
		this.httpHosts = httpHosts;
		this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
	}

	@Override
	public RestHighLevelClient createClient(Map<String, String> clientConfig) throws IOException {
		RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
		restClientFactory.configureRestClientBuilder(builder);

		RestHighLevelClient rhlClient = new RestHighLevelClient(builder);

		if (LOG.isInfoEnabled()) {
			LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHosts);
		}

		if (!rhlClient.ping()) {
			throw new RuntimeException("There are no reachable Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHosts.toString());
		}

		return rhlClient;
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder(client::bulkAsync, listener);
	}

	@Override
	public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return bulkItemResponse.getFailure().getCause();
		}
	}

	@Override
	public void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable BulkFlushBackoffPolicy flushBackoffPolicy) {

		BackoffPolicy backoffPolicy;
		if (flushBackoffPolicy != null) {
			switch (flushBackoffPolicy.getBackoffType()) {
				case CONSTANT:
					backoffPolicy = BackoffPolicy.constantBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
					break;
				case EXPONENTIAL:
				default:
					backoffPolicy = BackoffPolicy.exponentialBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
			}
		} else {
			backoffPolicy = BackoffPolicy.noBackoff();
		}

		builder.setBackoffPolicy(backoffPolicy);
	}

	@Override
	public RequestIndexer createBulkProcessorIndexer(BulkProcessor bulkProcessor) {
		return new Elasticsearch6BulkProcessorIndexer(bulkProcessor);
	}
}
