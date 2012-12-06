package de.strud.importer;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;
import org.apache.log4j.Logger;

/**
 * A simple riak implementation of an importer, it can be used with one host only or with a comma separated list of hosts.
 *
 * User: strud
 */
public class RiakImporter implements DBImporter {

    private static final Logger LOG = Logger.getLogger(RiakImporter.class);

    private IRiakClient client;

    private Bucket wikipediaBucket;

    public RiakImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            if (host.contains(",")) {
                String[] hosts = host.split(",");
                PBClusterConfig conf = new PBClusterConfig(200);
                for (String partHost : hosts) {
                    conf.addClient(new PBClientConfig.Builder().withHost(partHost).withPort(port).build());
                }
                this.client = RiakFactory.newClient(conf);
            } else {
                this.client = RiakFactory.pbcClient(host, port);
            }
            this.wikipediaBucket = this.client.fetchBucket("wikipedia").execute();
        } catch (RiakException e) {
            throw new DBImporterInitializationException("Cannot init riak connection.", e);
        }
    }

    @Override
    public boolean importDocument(Document document) {
        try {
            String[] parts = document.getUrl().split("/");
            this.wikipediaBucket.store(parts[parts.length - 1], document.getTitle() + "\n" + document.getText()).execute();
        } catch (RiakRetryFailedException e) {
            LOG.error("Cannot store document in riak.", e);
            return false;
        }
        return true;
    }
}
