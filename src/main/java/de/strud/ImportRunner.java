package de.strud;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public static void main(String[] args) {
        Options opts = parseArguments(args);
        LOG.info("Starting import process, using file {}.", opts.file());
        try (FileInputStream fin = new FileInputStream(new File(opts.file()))) {
            XMLParser parser = new XMLParser(fin, 1);
            Importer importer;
            switch (opts.mode().toLowerCase()) {
                case "mongo":
                    importer = new Importer(new MongoDBImporter(opts.host(), opts.port()));
                    break;
                case "redis":
                    importer = new Importer(new RedisImporter(opts.host(), opts.port()));
                    break;
                case "mysql":
                    importer = new Importer(new MysqlImporter(opts.host(), opts.port()));
                    break;
                case "postgresql":
                    importer = new Importer(new PostresqlImporter(opts.host(), opts.port()));
                    break;
                case "elasticsearch":
                    importer = new Importer(new ElasticSearchImporter(opts.host(), opts.port()));
                    break;
                default:
                    LOG.error("Cannot create concrete importer for mode '{}', aborting.", opts.mode());
                    System.exit(-1);
                    return;
            }
            LOG.info("Start parsing documents.");
            while (!parser.isRead()) {
                List<Document> documents = parser.parse(100000);
                LOG.info("Parsed batch of {} documents, starting import.", documents.size());
                importer.importDocuments(documents);
                LOG.info("Imported batch of {} documents.", documents.size());
            }
        } catch (DBImporterInitializationException e) {
            LOG.error("Cannot connect to db.", e);
            System.exit(-1);
        } catch (FileNotFoundException e) {
            LOG.error("Cannot find selected xml file.", e);
            System.exit(-1);
        } catch (IOException e) {
            LOG.error("Cannot close selected xml file.", e);
            System.exit(-1);
        } catch (XMLStreamException e) {
            LOG.error("Error when parsing xml.", e);
        }

        LOG.info("Import finished, bye bye.");
        System.exit(0);
    }

    private static Options parseArguments(final String[] args) {
        Map<String, String> parsed = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + argument);
            }

            String option = argument.substring(2);
            String value;
            int separator = option.indexOf('=');
            if (separator >= 0) {
                value = option.substring(separator + 1);
                option = option.substring(0, separator);
            } else {
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for option --" + option);
                }
                value = args[++index];
            }
            parsed.put(option, value);
        }

        String file = requireOption(parsed, "file");
        String host = parsed.getOrDefault("host", "localhost");
        String mode = requireOption(parsed, "mode");
        int port = Integer.parseInt(requireOption(parsed, "port"));
        return new Options(file, host, port, mode);
    }

    private static String requireOption(final Map<String, String> parsed, final String option) {
        String value = parsed.get(option);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required option --" + option);
        }
        return value;
    }

    private record Options(String file, String host, int port, String mode) {
    }
}
