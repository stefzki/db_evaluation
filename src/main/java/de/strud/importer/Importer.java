package de.strud.importer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import de.strud.data.Document;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class contains the basic importer logic. It passes all document objects to a specific DBImporter implementation.
 *
 * User: strud
 */
public class Importer {

    private static final Logger LOG = Logger.getLogger(Importer.class);

    private final DBImporter importer;

    private Meter importMeter;


    public Importer(final DBImporter importer) {
        this.importer = importer;
        this.importMeter = Metrics.newMeter(new MetricName("importer", "insert", "rate"), "inserts", TimeUnit.SECONDS);
    }

    public boolean importDocuments(final List<Document> documents) {
        boolean imported = true;
        for (Document doc : documents) {
            imported &= this.importer.importDocument(doc);
            if (this.importMeter.count() % 50000 == 0) {
                LOG.info("Inserted " + this.importMeter.count() + " from " + documents.size() + " current rate " + this.importMeter.oneMinuteRate() + " docs/sec; est. " + ((documents.size() - this.importMeter.count()) / this.importMeter.oneMinuteRate()) / 60 + " minutes to go.");
            }
        }
        return imported;
    }
}
