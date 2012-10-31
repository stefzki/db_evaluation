package de.strud.importer;

import com.mongodb.*;
import de.strud.data.Document;
import org.apache.log4j.Logger;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * A mongodb specific implementation, that maps Document objects to a mongodb compatible format and stores these objects
 * in the given mongodb instance.
 *
 * User: strud
 */
public class MongoDBImporter implements DBImporter {

    private static final Logger LOG = Logger.getLogger(MongoDBImporter.class);

    private final Mongo mongo;

    private final DBCollection collection;


    public MongoDBImporter(final String host, final int port) throws UnknownHostException {
        this.mongo = new Mongo(host, port);
        DB database = this.mongo.getDB("wikipedia");
        this.collection = database.getCollection("articles");
    }

    @Override
    public boolean importDocument(final Document document) {
        boolean success = true;
        Map<String, String> mapped = new HashMap<>();
        mapped.put("url", document.getUrl());
        mapped.put("title", document.getTitle());
        mapped.put("text", document.getText());
        WriteResult result = this.collection.save(new BasicDBObject(mapped));
        if (result.getError() != null) {
            LOG.error("Unable to store document " + document + " reason: " + result.getError() + ".");
            success = false;
        }
        return success;
    }
}
