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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.elasticsearch.action.bulk.BulkRequest;
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

  private static final Logger LOG = LoggerFactory.getLogger(RestHighLevelElasticsearchClient.class);

  private final RestHighLevelClient client;

  // visible for testing
  public RestHighLevelElasticsearchClient(RestHighLevelClient client) {
      this.client = client;
  }

  public RestHighLevelElasticsearchClient(String address) {
    try {
      this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address)));
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

      String address =
          config.getString(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);

      this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address)));
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
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
        try {
          client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
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
    client.bulkAsync(bulk, RequestOptions.DEFAULT, listener);
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      LOG.error("Cannot close {}", e);
    }
  }
}
