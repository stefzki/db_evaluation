package de.strud.importer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;

import de.strud.data.Document;

/**
 * A mongodb specific implementation, that maps Document objects to a mongodb compatible format and stores these objects
 * in the given mongodb instance.
 *
 * User: strud
 */
public class MongoVirtualShardingKeyDBImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(MongoVirtualShardingKeyDBImporter.class);

    private final Mongo mongo;

    private final DBCollection collection;

    private final int mongoShards;

    private int imported = 0;

    public MongoVirtualShardingKeyDBImporter(final String host, final int port, final int mongoShards) throws UnknownHostException {
        this.mongoShards = mongoShards;
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
        mapped.put("sKey", String.valueOf(this.imported % this.mongoShards));
        WriteResult result = this.collection.save(new BasicDBObject(mapped));
        if (result.getError() != null) {
            LOG.error("Unable to store document " + document + " reason: " + result.getError() + ".");
            success = false;
        }
        this.imported++;
        return success;
    }

}
