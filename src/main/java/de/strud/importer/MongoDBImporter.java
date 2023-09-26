package de.strud.importer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;

/**
 * A mongodb specific implementation, that maps Document objects to a mongodb compatible format and stores these objects
 * in the given mongodb instance.
 *
 * User: strud
 */
public class MongoDBImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(MongoDBImporter.class);

    private final MongoClient mongo;

    private final DBCollection collection;


    public MongoDBImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            this.mongo = new MongoClient(host, port);
            DB database = this.mongo.getDB("wikipedia_live");
            this.collection = database.getCollection("articles");
        } catch (UnknownHostException e) {
            throw new DBImporterInitializationException("Cannot connect to mongo.", e);
        }
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
