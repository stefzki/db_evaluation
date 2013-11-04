package de.strud.importer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;
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
public class MongoDBConferenceImporter implements DBImporter {

    private static final Logger LOG = Logger.getLogger(MongoDBConferenceImporter.class);

    private final MongoClient mongo;

    private final DBCollection collection_id;
    private final DBCollection collection_url;
    private final DBCollection collection_title;
    private final DBCollection collection_title_hash;


    public MongoDBConferenceImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            this.mongo = new MongoClient(host, port);
            DB databaseId = this.mongo.getDB("wikipedia_id");
            DB databaseUrl = this.mongo.getDB("wikipedia_url");
            DB databaseTitle = this.mongo.getDB("wikipedia_title");
            DB databaseTitleHash = this.mongo.getDB("wikipedia_title_hash");
            this.collection_id = databaseId.getCollection("articles");
            this.collection_url = databaseUrl.getCollection("articles");
            this.collection_title = databaseTitle.getCollection("articles");
            this.collection_title_hash = databaseTitleHash.getCollection("articles");
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
        WriteResult result_id = this.collection_id.save(new BasicDBObject(mapped));
        if (result_id.getError() != null) {
            LOG.error("Unable to store document " + document + " reason: " + result_id.getError() + ".");
            success = false;
        }
        WriteResult result_title = this.collection_title.save(new BasicDBObject(mapped));
        if (result_title.getError() != null) {
          LOG.error("Unable to store document " + document + " reason: " + result_title.getError() + ".");
          success = false;
        }
        WriteResult result_title_hash = this.collection_title_hash.save(new BasicDBObject(mapped));
        if (result_title_hash.getError() != null) {
          LOG.error("Unable to store document " + document + " reason: " + result_title_hash.getError() + ".");
          success = false;
        }
        WriteResult result_url = this.collection_url.save(new BasicDBObject(mapped));
        if (result_url.getError() != null) {
          LOG.error("Unable to store document " + document + " reason: " + result_url.getError() + ".");
          success = false;
        }
        return success;
    }
}
