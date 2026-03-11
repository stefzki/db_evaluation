package de.strud.importer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

import de.strud.exceptions.DBImporterInitializationException;

/**
 * A mongodb specific implementation, that maps Document objects to a mongodb compatible format and stores these objects
 * in the given mongodb instance.
 *
 * User: strud
 */
public class MongoVirtualShardingKeyDBImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(MongoVirtualShardingKeyDBImporter.class);

    private final MongoClient mongo;

    private final MongoCollection<Document> collection;

    private final int mongoShards;

    private int imported = 0;

    public MongoVirtualShardingKeyDBImporter(final String host, final int port, final int mongoShards)
            throws DBImporterInitializationException {
        try {
            this.mongoShards = mongoShards;
            this.mongo = MongoClients.create("mongodb://" + host + ":" + port);
            this.collection = this.mongo.getDatabase("wikipedia").getCollection("articles");
            this.collection.countDocuments();
        } catch (MongoException e) {
            throw new DBImporterInitializationException("Cannot connect to mongo.", e);
        }
    }

    @Override
    public boolean importDocument(final de.strud.data.Document document) {
        try {
            this.collection.insertOne(toMongoDocument(document, this.imported % this.mongoShards));
            this.imported++;
            return true;
        } catch (MongoException e) {
            LOG.error("Unable to store document {}.", document, e);
            this.imported++;
            return false;
        }
    }

    private static Document toMongoDocument(final de.strud.data.Document document, final int shardKey) {
        return new Document("url", document.getUrl())
                .append("title", document.getTitle())
                .append("text", document.getText())
                .append("sKey", Integer.toString(shardKey));
    }
}
