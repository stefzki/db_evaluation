package de.strud;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;
import de.strud.data.Document;
import de.strud.importer.Importer;
import de.strud.importer.MongoDBImporter;
import de.strud.xmlparser.XMLParser;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Main class, opens a file and starts parsing and importer db insertion.
 * User: strud
 */
public class ImportRunner {

    private static final Logger LOG = Logger.getLogger(ImportRunner.class);

    interface Options {

        @Option String getFile();

        @Option String getHost();

        @Option int getPort();

    }

    public static void main(String[] args) {
        Options opts = CliFactory.parseArguments(Options.class, args);
        LOG.info("Starting import process, using file " + opts.getFile() + ".");
        try {
            FileInputStream fin = new FileInputStream(new File(opts.getFile()));
            XMLParser parser = new XMLParser(fin);
            Importer importer = new Importer(new MongoDBImporter(opts.getHost(), opts.getPort()));
            LOG.info("Start parsing documents.");
            while (!parser.isRead()) {
                List<Document> documents = parser.parse(100000);
                LOG.info("Parsed batch of " + documents.size() + " documents, starting import.");
                importer.importDocuments(documents);
                LOG.info("Imported batch of " + documents.size() + " documents.");
            }
            IOUtils.closeQuietly(fin);
        } catch (UnknownHostException e) {
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
