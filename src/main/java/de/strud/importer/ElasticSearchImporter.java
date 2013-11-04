package de.strud.importer;

import de.strud.data.Document;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

/**
 * User: strud
 */
public class ElasticSearchImporter implements DBImporter {

    private static final Logger LOG = Logger.getLogger(ElasticSearchImporter.class);

    private final Node node;

    public ElasticSearchImporter() {
        this.node = NodeBuilder.nodeBuilder().clusterName("elasticsearch_sm").client(true).node();
    }


    @Override
    public boolean importDocument(Document document) {
        try {
        Client client = this.node.client();

        client.prepareIndex("wikipedia", "article").setSource(
                XContentFactory.jsonBuilder().startObject()
                        .field("url", document.getUrl())
                        .field("title", document.getTitle())
                        .field("text", document.getText())
                        .endObject()).execute().actionGet();
        } catch (IOException e) {
            LOG.error("Cannot index document.", e);
            return false;
        }
        return true;
    }
}
