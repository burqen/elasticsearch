/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

public class SyntheticIdIT extends ESRestTestCase {
    private static final String BWC_NODES_VERSION = System.getProperty("tests.bwc_nodes_version");
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());
    private static final Instant TIMESTAMP = Instant.now();

    public void testCreate93Index() throws Exception {
        assertTrue("Feature flag from 9.3 must be enabled", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        assumeFalse("old news is empty", nodes.getBWCNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());

        Map<String, String> nodeNameToESVersion = new HashMap<>();
        nodes.getNewNodes().forEach(node -> nodeNameToESVersion.put(node.nodeName(), node.version()));
        nodes.getBWCNodes().forEach(node -> nodeNameToESVersion.put(node.nodeName(), node.version()));

        final String dataStream = "synthetic-id-index";
        setupDataStream(dataStream);

        ensureGreen(client(), dataStream);

        String indexName = getIndexName(dataStream);

        String settingsValue = getSettingValue(indexName, "index.mapping.use_synthetic_id");
        assertThat(settingsValue, equalTo("true"));

        AllocationVersionMix mix = getShardAllocationVersionMix(indexName, nodeNameToESVersion);
        logger.info("Allocated version mix: {}", mix);

        indexDocs(dataStream, 10);
        ensureGreen(client(), dataStream);

        assertOK(client().performRequest(new Request("POST", dataStream + "/_refresh")));
    }

    private AllocationVersionMix getShardAllocationVersionMix(String indexName, Map<String, String> nameToVersion) throws IOException {
        ObjectPath shards = executeAndLog("GET", "/_cat/shards/" + indexName + "?format=json");
        logger.info("Shards for index {}: {}", indexName, shards.toString());
        assertThat(shards.evaluateArraySize(""), equalTo(2));
        Set<String> allocatedVersions = new HashSet<>();
        String versionShard0 = nameToVersion.get(shards.evaluate("0.node"));
        String versionShard1 = nameToVersion.get(shards.evaluate("1.node"));
        allocatedVersions.add(versionShard0);
        allocatedVersions.add(versionShard1);
        assertThat(versionShard0, either(equalTo("9.4.0")).or(equalTo("9.3.0")));

        AllocationVersionMix mix;
        if (versionShard0.equals(versionShard1) == false) {
            mix = AllocationVersionMix.MIXED;
        } else if (versionShard0.equals("9.4.0")) {
            mix = AllocationVersionMix.ALL_ON_NEW;
        } else {
            mix = AllocationVersionMix.ALL_ON_OLD;
        }
        return mix;
    }

    private String getSettingValue(String indexName, String setting) throws IOException {
        ObjectPath indexSettingsPath = executeAndLog("GET", "/" + indexName + "/_settings/" + setting);
        assertNotNull(indexSettingsPath);
        String settingsValue = indexSettingsPath.evaluate(indexName.replace(".", "\\.") + ".settings." + setting);
        assertNotNull(settingsValue);
        return settingsValue;
    }

    private String getIndexName(String dataStream) throws IOException {
        ObjectPath dataStreamResp = executeAndLog("GET", "/_data_stream/" + dataStream);
        assertNotNull(dataStreamResp);
        assertThat(dataStreamResp.evaluateArraySize("data_streams"), equalTo(1));
        assertThat(dataStreamResp.evaluateArraySize("data_streams.0.indices"), equalTo(1));
        return dataStreamResp.evaluate("data_streams.0.indices.0.index_name");
    }

    private static void setupDataStream(String dataStream) throws IOException {
        final var settingComponentTemplate = """
            {
                "template": {
                    "settings": {
                        "index.number_of_replicas": 1,
                        "index.number_of_shards": 1,
                        "index.mode": "time_series",
                        "index.routing_path": "hostname",
                        "index.mapping.use_synthetic_id": true
                    }
                }
            }
            """;
        Request settingsComponentRequest = new Request("PUT", "_component_template/my-settings");
        settingsComponentRequest.setJsonEntity(settingComponentTemplate);
        client().performRequest(settingsComponentRequest);

        final var mappingComponentTemplate = """
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                            "type": "date"
                        },
                        "hostname": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "metric": {
                            "properties": {
                                "field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                },
                                "value": {
                                    "type": "integer",
                                    "time_series_metric": "counter"
                                }
                            }
                        }
                        }
                    }
                }
            }
            """;
        Request mappingComponentRequest = new Request("PUT", "_component_template/my-mappings");
        mappingComponentRequest.setJsonEntity(mappingComponentTemplate);
        client().performRequest(mappingComponentRequest);

        final var indexTemplate = String.format(Locale.ROOT, """
            {
                "index_patterns": ["%s"],
                "data_stream": { },
                "composed_of": [ "my-mappings", "my-settings" ]
            }
            """, dataStream);
        Request templateRequest = new Request("PUT", "/_index_template/template-" + dataStream);
        templateRequest.setJsonEntity(indexTemplate);
        client().performRequest(templateRequest);

        client().performRequest(new Request("PUT", "/_data_stream/" + dataStream));
    }

    private ObjectPath executeAndLog(String method, String endpoint) throws IOException {
        try {
            Response response = client().performRequest(new Request(method, endpoint));
            ObjectPath fromResponse = asObjectPath(response);
            logger.info(method + " " + endpoint + " objectPath: \n" + fromResponse);
            return fromResponse;
        } catch (Exception e) {
            logger.info(method + " " + endpoint + " exception: " + e.getMessage(), e);
        }
        return null;
    }

    private static ObjectPath asObjectPath(Response response) throws IOException {
        return ObjectPath.createFromResponse(response);
    }

    private static String asString(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }

    private static boolean isJson(Response response) {
        return response.getHeader("Content-Type").contains("application/json");
    }

    private static XContentTestUtils.JsonMapView asJson(Response response) throws IOException {
        return XContentTestUtils.createJsonMapView(response.getEntity().getContent());
    }

    // todo
    // public void testCreate93IndexNewNode() throws Exception {
    // public void testCreate94IndexNewNode() throws Exception {
    // public void testCreate94IndexOldNode() throws Exception {

    // todo: tmp
    private int indexDocs(String index, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Instant time = TIMESTAMP.plus(i, ChronoUnit.SECONDS);

            Request request = new Request("POST", index + "/_doc/");
            String json = String.format(Locale.ENGLISH, """
                {
                    "@timestamp": "%s",
                    "hostname": "host",
                    "metric": {"field": "cpu-load", "value": %d}
                }
                """, time, i);
            // todo remove log
            // logger.info("try to index {}", json);
            request.setJsonEntity(json);
            Response response = assertOK(client().performRequest(request));
            // logger.info("... done");
            ObjectPath fromResponse = ObjectPath.createFromResponse(response);
            logger.info("Create doc response objectPath: \n" + fromResponse);
        }
        return numDocs;
    }

    // todo: tmp
    private List<Shard> buildShards(String index, MixedClusterTestNodes nodes, RestClient client) throws IOException {
        Request request = new Request("GET", index + "/_stats");
        request.addParameter("level", "shards");
        Response response = client.performRequest(request);
        List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards.0");
        ArrayList<Shard> shards = new ArrayList<>();
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            final MixedClusterTestNode node = nodes.getSafe(nodeId);
            final SeqNoStats seqNoStats;
            Integer maxSeqNo = ObjectPath.evaluate(shard, "seq_no.max_seq_no");
            Integer localCheckpoint = ObjectPath.evaluate(shard, "seq_no.local_checkpoint");
            Integer globalCheckpoint = ObjectPath.evaluate(shard, "seq_no.global_checkpoint");
            seqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
            shards.add(new Shard(node, primary, seqNoStats));
        }
        logger.info("shards {}", shards);
        return shards;
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }

    private void assertVersion(final String index, final int docId, final String preference, final int expectedVersion) throws IOException {
        Request request = new Request("GET", index + "/_doc/" + docId);
        request.addParameter("preference", preference);

        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualVersion = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("_version").toString());
        assertThat("version mismatch for doc [" + docId + "] preference [" + preference + "]", actualVersion, equalTo(expectedVersion));
    }

    private MixedClusterTestNodes buildNodeAndVersions() throws IOException {
        return MixedClusterTestNodes.buildNodes(client(), BWC_NODES_VERSION);
    }

    private record Shard(MixedClusterTestNode node, boolean primary, SeqNoStats seqNoStats) {}

    private enum AllocationVersionMix {
        MIXED,
        ALL_ON_NEW,
        ALL_ON_OLD
    }
}
