/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
//import io.confluent.connect.elasticsearch.bulk.BulkRequest;
//import io.confluent.connect.elasticsearch.bulk.BulkResponse;
import org.apache.http.HttpHost;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RestHighLevelElasticsearchClient implements ElasticsearchClient {

  // visible for testing
  protected static final String MAPPER_PARSE_EXCEPTION
      = "mapper_parse_exception";
  protected static final String VERSION_CONFLICT_ENGINE_EXCEPTION
      = "version_conflict_engine_exception";

  private static final Logger LOG = LoggerFactory.getLogger(RestHighLevelElasticsearchClient.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RestHighLevelClient client;
//  private final Version version;

  // visible for testing
  public RestHighLevelElasticsearchClient(RestHighLevelClient client) {
//    try {
      this.client = client;
//      this.version = getServerVersion();
//    } catch (IOException e) {
//      throw new ConnectException(
//          "Couldn't start ElasticsearchSinkTask due to connection error:",
//          e
//      );
//    }
  }

  // visible for testing
  public RestHighLevelElasticsearchClient(String address) {
    try {
//      JestClientFactory factory = new JestClientFactory();
//      factory.setHttpClientConfig(new HttpClientConfig.Builder(address)
//          .multiThreaded(true)
//          .build()
//      );
      this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address)));
//      this.version = getServerVersion();
//    } catch (IOException e) {
//      throw new ConnectException(
//          "Couldn't start ElasticsearchSinkTask due to connection error:",
//          e
//      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  public RestHighLevelElasticsearchClient(Map<String, String> props) {
    try {
      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
//      final int connTimeout = config.getInt(
//          ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
//      final int readTimeout = config.getInt(
//          ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG);

      String address =
          config.getString(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);

      this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address)));

//      JestClientFactory factory = new JestClientFactory();
//      factory.setHttpClientConfig(new HttpClientConfig.Builder(address)
//          .connTimeout(connTimeout)
//          .readTimeout(readTimeout)
//          .multiThreaded(true)
//          .build()
//      );
//      this.client = factory.getObject();
//      this.version = getServerVersion();
//    } catch (IOException e) {
//      throw new ConnectException(
//          "Couldn't start ElasticsearchSinkTask due to connection error:",
//          e
//      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  /*
   * This method uses the NodesInfo request to get the server version, which is expected to work
   * with all versions of Elasticsearch.
   */
//  private Version getServerVersion() throws IOException {
//    // Default to newest version for forward compatibility
//    Version defaultVersion = Version.ES_V6;
//
//    NodesInfo info = new NodesInfo.Builder().addCleanApiParameter("version").build();
//    client.
//    JsonObject result = client.execute(info).getJsonObject();
//    if (result == null) {
//      LOG.warn("Couldn't get Elasticsearch version, result is null");
//      return defaultVersion;
//    }
//
//    JsonObject nodesRoot = result.get("nodes").getAsJsonObject();
//    if (nodesRoot == null || nodesRoot.entrySet().size() == 0) {
//      LOG.warn("Couldn't get Elasticsearch version, nodesRoot is null or empty");
//      return defaultVersion;
//    }
//
//    JsonObject nodeRoot = nodesRoot.entrySet().iterator().next().getValue().getAsJsonObject();
//    if (nodeRoot == null) {
//      LOG.warn("Couldn't get Elasticsearch version, nodeRoot is null");
//      return defaultVersion;
//    }
//
//    String esVersion = nodeRoot.get("version").getAsString();
//    if (esVersion == null) {
//      LOG.warn("Couldn't get Elasticsearch version, version is null");
//      return defaultVersion;
//    } else if (esVersion.startsWith("1.")) {
//      return Version.ES_V1;
//    } else if (esVersion.startsWith("2.")) {
//      return Version.ES_V2;
//    } else if (esVersion.startsWith("5.")) {
//      return Version.ES_V5;
//    } else if (esVersion.startsWith("6.")) {
//      return Version.ES_V6;
//    }
//    return defaultVersion;
//  }

  public Version getVersion() {
    return null;
  }

  private boolean indexExists(String index) {
    GetIndexRequest request = new GetIndexRequest();
    request.indices(index);
    try {
      return client.indices().exists(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void createIndices(Set<String> indices) {
    for (String index : indices) {
      if (!indexExists(index)) {
//        CreateIndex createIndex = new CreateIndex.Builder(index).build();
        try {
          client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
//          if (!result.) {
//            // Check if index was created by another client
//            if (!indexExists(index)) {
//              String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
//              throw new ConnectException("Could not create index '" + index + "'" + msg);
//            }
//          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    }
  }

  public void createMapping(String index, String type, Schema schema) throws IOException {
    PutMappingRequest request = new PutMappingRequest(index);
    request.type(type);
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(type, Mapping.inferMapping(this, schema));
    request.source(obj.toString(), XContentType.JSON);
  }

  /**
   * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
   */
  public boolean mappingExists(String index, String type) throws IOException {
    GetMappingsRequest request = new GetMappingsRequest();
    request.indices(index);
    request.types(type);
    GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
    return !getMappingResponse.getMappings().isEmpty();
  }

  public BulkRequest createBulkRequest(List<IndexableRecord> batch) {
    BulkRequest request = new BulkRequest();
    for (IndexableRecord record : batch) {
      if (record.payload != null) {
        request.add(toDeleteRequest(record));
      } else {
        request.add(toIndexRequest(record));
      }
    }
    return request;
  }

//  // visible for testing
//  protected BulkableAction toBulkableAction(IndexableRecord record) {
//    // If payload is null, the record was a tombstone and we should delete from the index.
//    return record.payload != null ? toIndexRequest(record) : toDeleteRequest(record);
//  }

  private DeleteRequest toDeleteRequest(IndexableRecord record) {
    return new DeleteRequest(record.key.index, record.key.type, record.key.id);
  }

  private IndexRequest toIndexRequest(IndexableRecord record) {
    IndexRequest indexRequest = new IndexRequest(record.key.index, record.key.type, record.key.id);
    if (record.version != null) {
      indexRequest.versionType(VersionType.EXTERNAL).version(record.version);
    }
    return indexRequest;
  }

  public void executeBulk(BulkRequest bulk, ActionListener listener) {

//    BulkResponse response =
            client.bulkAsync(bulk, RequestOptions.DEFAULT, listener);


//    final BulkResult result = client.execute(((JestBulkRequest) bulk).getBulk());

//    if (!response.hasFailures()) {
//      return response;
//    }
//
//    boolean retriable = true;
//
//    final List<Key> versionConflicts = new ArrayList<>();
//    final List<String> errors = new ArrayList<>();
//
//    for (BulkItemResponse item : response.getItems()) {
//      if (item.isFailed()) {
////        final ObjectNode parsedError = (ObjectNode) OBJECT_MAPPER.readTree(item.error);
//        BulkItemResponse.Failure failure = item.getFailure();
//        final String errorType = failure.getType();
//        if ("version_conflict_engine_exception".equals(errorType)) {
//          versionConflicts.add(new Key(item.getIndex(), item.getType(), item.getId()));
//        } else if ("mapper_parse_exception".equals(errorType)) {
//          retriable = false;
//          errors.add(item.getFailureMessage());
//        } else {
//          errors.add(item.getFailureMessage());
//        }
//      }
//    }
//
//    if (!versionConflicts.isEmpty()) {
//      LOG.debug("Ignoring version conflicts for items: {}", versionConflicts);
//      if (errors.isEmpty()) {
//        // The only errors were version conflicts
//        return response;
//      }
//    }
//
////    final String errorInfo = errors.isEmpty() ? response.buildFailureMessage() : errors.toString();
//
//    return response;
  }

  public JsonObject search(String query, String index, String type) throws IOException {
//    SearchRequest searchRequest = new SearchRequest();
//    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//    searchRequest.source(searchSourceBuilder);
//    if (index != null) {
//      searchRequest.indices(index);
//    }
//    if (type != null) {
//      searchRequest.types(type);
//    }
//    SearchResponse response = client.search(searchRequest);
//    return response.getHits();

    return null;
//    final SearchResult result = client.execute(search.build());

//    return result.getJsonObject();
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      LOG.error("Cannot close {}", e);
    }
  }
}
