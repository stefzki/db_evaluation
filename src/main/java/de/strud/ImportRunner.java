package de.strud;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;
import de.strud.importer.ElasticSearchImporter;
import de.strud.importer.Importer;
import de.strud.importer.MongoDBImporter;
import de.strud.importer.MysqlImporter;
import de.strud.importer.PostresqlImporter;
import de.strud.importer.RedisImporter;
import de.strud.xmlparser.XMLParser;

/**
 * Main class, opens a file and starts parsing and importer db insertion. Change implementations to use different importers.
 *
 * User: strud
 */
public class ImportRunner {

    private static final Logger LOG = LogManager.getLogger(ImportRunner.class);

    interface Options {

        @Option String getFile();

        @Option String getHost();

        @Option int getPort();

        @Option String getMode();

    }

    public static void main(String[] args) {
        Options opts = CliFactory.parseArguments(Options.class, args);
        LOG.info("Starting import process, using file " + opts.getFile() + ".");
        try {
            FileInputStream fin = new FileInputStream(new File(opts.getFile()));
            XMLParser parser = new XMLParser(fin, 1);
            Importer importer = null;
            switch (opts.getMode().toLowerCase()) {
                case "mongo":
                    importer = new Importer(new MongoDBImporter(opts.getHost(), opts.getPort()));
                    break;
                case "redis":
                    importer = new Importer(new RedisImporter(opts.getHost(), opts.getPort()));
                    break;
                case "mysql":
                    importer = new Importer(new MysqlImporter(opts.getHost(), opts.getPort()));
                    break;
                case "postgresql":
                    importer = new Importer(new PostresqlImporter(opts.getHost(), opts.getPort()));
                    break;
                case "elasticsearch":
                    importer = new Importer(new ElasticSearchImporter(opts.getHost(), opts.getPort()));
                    break;
                default:
                    LOG.error("Cannot create concrete importer, aborting.");
                    System.exit(-1);
            }
            LOG.info("Start parsing documents.");
            while (!parser.isRead()) {
                List<Document> documents = parser.parse(100000);
                LOG.info("Parsed batch of " + documents.size() + " documents, starting import.");
                importer.importDocuments(documents);
                LOG.info("Imported batch of " + documents.size() + " documents.");
            }
            IOUtils.closeQuietly(fin);
        } catch (DBImporterInitializationException e) {
            LOG.error("Cannot connect to db.", e);
            System.exit(-1);
        } catch (FileNotFoundException e) {
            LOG.error("Cannot find selected xml file.", e);
            System.exit(-1);
        } catch (XMLStreamException e) {
            LOG.error("Error when parsing xml.", e);
        }

        LOG.info("Import finished, bye bye.");
        System.exit(0);
    }
}
