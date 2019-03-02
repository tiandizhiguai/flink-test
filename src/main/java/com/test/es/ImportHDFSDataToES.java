package com.test.es;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.test.es.ElasticsearchOutputFormat.Builder;

public class ImportHDFSDataToES {

	private static final Logger LOG = LoggerFactory.getLogger(ImportHDFSDataToES.class);

  // --input-path hdfs://namenode01.td.com/tmp/data --es-http-hosts 172.23.4.141 --es-http-port 9200 --es-index a_multi_val --es-type a_my_type
  public static void main(String[] args) throws Exception {
    LOG.info("Input params: " + Arrays.asList(args));
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 5) {
      System.out.println("Missing parameters!\n" +
          "Usage: batch " +
          "--input-path <hdfsFile> " +
          "--es-http-hosts <esHttpHosts> " +
          "--es-http-port <esHttpPort> " +
          "--es-index <esIndex> " +
          "--es-type <esType> " +
          "--bulk-flush-interval-millis <bulkFlushIntervalMillis>" +
          "--bulk-flush-max-size-mb <bulkFlushMaxSizeMb>" +
          "--bulk-flush-max-actions <bulkFlushMaxActions>");
      return;
    }

    String file = parameterTool.getRequired("input-path");

    final ElasticsearchSinkFunction<String> elasticsearchSinkFunction = new ElasticsearchSinkFunction<String>() {

      @Override
      public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element, parameterTool));
      }

      private IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
        LOG.info("Create index req: " + element);
        JSONObject o = JSONObject.parseObject(element);
        return Requests.indexRequest()
                .index(parameterTool.getRequired("es-index"))
                .type(parameterTool.getRequired("es-type"))
                .source(o);
      }
    };

    String esHttpHosts = parameterTool.getRequired("es-http-hosts");
    LOG.info("Config: esHttpHosts=" + esHttpHosts);
    int esHttpPort = parameterTool.getInt("es-http-port", 9200);
    LOG.info("Config: esHttpPort=" + esHttpPort);

    final List<HttpHost> httpHosts = Arrays.asList(esHttpHosts.split(","))
            .stream()
            .map(host -> new HttpHost(host, esHttpPort, "http"))
            .collect(Collectors.toList());

    int bulkFlushMaxSizeMb = parameterTool.getInt("bulk-flush-max-size-mb", 10);
    int bulkFlushIntervalMillis = parameterTool.getInt("bulk-flush-interval-millis", 10 * 1000);
    int bulkFlushMaxActions = parameterTool.getInt("bulk-flush-max-actions", 1);

    final ElasticsearchOutputFormat outputFormat = new Builder<>(httpHosts, elasticsearchSinkFunction)
            .setBulkFlushBackoff(true)
            .setBulkFlushBackoffRetries(2)
            .setBulkFlushBackoffType(ElasticsearchApiCallBridge.FlushBackoffType.EXPONENTIAL)
            .setBulkFlushMaxSizeMb(bulkFlushMaxSizeMb)
            .setBulkFlushInterval(bulkFlushIntervalMillis)
            .setBulkFlushMaxActions(bulkFlushMaxActions)
            .build();

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.readTextFile(file)
        .filter(line -> !line.isEmpty())
        .map(line -> line)
        .output(outputFormat);

    final String jobName = ImportHDFSDataToES.class.getSimpleName();
    env.execute(jobName);
  }
}
