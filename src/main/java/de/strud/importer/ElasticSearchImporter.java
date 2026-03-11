package de.strud.importer;

import java.io.IOException;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import de.strud.data.Document;

/**
 * User: strud
 */
public class ElasticSearchImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(ElasticSearchImporter.class);

    private final ElasticsearchTransport transport;

    private final ElasticsearchClient client;

    public ElasticSearchImporter(String host, int port) {
        Rest5Client httpClient = Rest5Client.builder(
                new HttpHost(host, port)
            ).build();

        transport = new Rest5ClientTransport(httpClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
    }


    @Override
    public boolean importDocument(Document document) {
        try {
            IndexRequest<Document> request = IndexRequest.of(i -> i
                .index("wikipedia")
                .id(document.getUrl())
                .document(document)
            );

            IndexResponse response = client.index(request);
        } catch (IOException e) {
            LOG.error("Cannot index document.", e);
            return false;
        }
        return true;
    }
}
