package de.strud.importer;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A redis specific implementation, that maps Document objects to a key (url) - value (document) compatible format and
 * stores these in the given redis instance.
 *
 * User: strud
 */
public class RedisImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(RedisImporter.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final JedisPool pool;


    public RedisImporter(final String host, final int port) throws DBImporterInitializationException {
        if (!new Jedis(host, port).ping().equalsIgnoreCase("pong")) {
            throw new DBImporterInitializationException("Cannot connect to redis.");
        }
        this.pool = new JedisPool(host, port);
    }

    @Override
    public boolean importDocument(final Document document) {

        boolean success = true;
        Jedis jedis = null;
        try {
            jedis = this.pool.getResource();
            jedis.set(document.getUrl(), JSON_MAPPER.writeValueAsString(document));
        } catch (IOException e) {
            LOG.error("Cannot write document " + document + " to redis.", e);
            success = false;
        } finally {
            if (jedis != null) {
                this.pool.returnResource(jedis);
            }
        }

        return success;
    }
}
