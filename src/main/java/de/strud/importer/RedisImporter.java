package de.strud.importer;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;

/**
 * A redis specific implementation, that maps Document objects to a key (url) - value (document) compatible format and
 * stores these in the given redis instance.
 *
 * User: strud
 */
public class RedisImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(RedisImporter.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Jedis jedis;


    public RedisImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            this.jedis = new Jedis(
                    host,
                    port,
                    DefaultJedisClientConfig.builder().build());
            if (!"PONG".equalsIgnoreCase(this.jedis.ping())) {
                throw new DBImporterInitializationException("Cannot connect to redis.");
            }
        } catch (JedisException e) {
            throw new DBImporterInitializationException("Cannot connect to redis.", e);
        }
    }

    @Override
    public boolean importDocument(final Document document) {
        try {
            this.jedis.set(document.getUrl(), JSON_MAPPER.writeValueAsString(document));
            return true;
        } catch (IOException | JedisException e) {
            LOG.error("Cannot write document {} to redis.", document, e);
            return false;
        }
    }
}
