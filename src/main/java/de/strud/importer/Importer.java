package de.strud.importer;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import de.strud.data.Document;

/**
 * This class contains the basic importer logic. It passes all document objects to a specific DBImporter implementation.
 *
 * User: strud
 */
public class Importer {

    private static final Logger LOG = LogManager.getLogger(Importer.class);

    private final DBImporter importer;

    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private final Meter importMeter;

    private final Timer importTimer;

    public Importer(final DBImporter importer) {
        this.importer = importer;
        this.importMeter = METRIC_REGISTRY.meter(MetricRegistry.name("importer", "insert", "rate"));
        this.importTimer = METRIC_REGISTRY.timer(MetricRegistry.name("importer", "insert", "timing"));
    }

    public boolean importDocuments(final List<Document> documents) {
        boolean imported = true;
        int importedDocs = 0;
        for (Document doc : documents) {
            importedDocs++;
            final Timer.Context context = this.importTimer.time();
            try {
                imported &= this.importer.importDocument(doc);
            } finally {
                context.stop();
            }
            this.importMeter.mark();
            if (this.importMeter.getCount() % 10000 == 0) {
                double oneMinuteRate = this.importMeter.getOneMinuteRate();
                double secondsRemaining = oneMinuteRate > 0
                        ? (documents.size() - importedDocs) / oneMinuteRate
                        : Double.POSITIVE_INFINITY;
                LOG.info(
                        "Inserted {} from {} documents (total {}), current average rate {} docs/sec; est. {} seconds to go, currently {} ms/doc (min: {}, max: {}).",
                        importedDocs,
                        documents.size(),
                        this.importMeter.getCount(),
                        oneMinuteRate,
                        secondsRemaining,
                        this.importTimer.getSnapshot().getMean() / 1_000_000.0,
                        this.importTimer.getSnapshot().getMin() / 1_000_000.0,
                        this.importTimer.getSnapshot().getMax() / 1_000_000.0);
            }
        }
        return imported;
    }
}
