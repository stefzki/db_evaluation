package de.strud.importer;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import de.strud.data.Document;

/**
 * User: strud
 */
public class ElasticSearchImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(ElasticSearchImporter.class);
    private final ElasticsearchTransport transport;

    public ElasticSearchImporter(String host, int port) {
        RestClient httpClient = RestClient.builder(
                new HttpHost(host, port)
            ).build();

        transport = new RestClientTransport(httpClient, new JacksonJsonpMapper());
    }


    @Override
    public boolean importDocument(Document document) {
        try {
            ElasticsearchClient esClient = new ElasticsearchClient(transport);
            IndexRequest<Document> request = IndexRequest.of(i -> i
                .index("wikipedia")
                .id(document.getUrl())
                .document(document)
            );

            IndexResponse response = esClient.index(request);
        } catch (IOException e) {
            LOG.error("Cannot index document.", e);
            return false;
        }
        return true;
    }
}
