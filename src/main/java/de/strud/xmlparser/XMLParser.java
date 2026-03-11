package de.strud.xmlparser;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import de.strud.data.Document;

/**
 * Simple stax parser, that parses wikipedia abstract dumps and transforms them into Document objects
 * (see http://dumps.wikimedia.org/enwiki/20121001/enwiki-20121001-abstract.xml).
 *
 * User: strud
 */
public class XMLParser {

    private static final Logger LOG = LogManager.getLogger(XMLParser.class);

    private final InputStream source;

    private final int every;

    private final XMLStreamReader streamReader;

    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private final Meter xmlMeter;

    public XMLParser(InputStream source, int every) throws XMLStreamException {
        if (source == null) {
            throw new IllegalArgumentException("InputStream cannot be null.");
        }
        this.source = source;
        this.xmlMeter = METRIC_REGISTRY.meter(MetricRegistry.name("xml", "parsing", "rate"));
        XMLInputFactory factory = XMLInputFactory.newInstance();
        this.streamReader = factory.createXMLStreamReader(this.source);
        this.every = every;
    }

    public List<Document> parse(final int limit) {
        List<Document> documents = new LinkedList<>();
        try {
            int parsed = 0;
            boolean inTitle = false;
            boolean inText = false;
            boolean inUrl = false;
            Document currentDoc = null;
            while(parsed < limit && this.streamReader.hasNext()){
                this.streamReader.next();
                if(this.streamReader.getEventType() == XMLStreamReader.START_ELEMENT){
                    if ("doc".equals(this.streamReader.getLocalName())) {
                        currentDoc = new Document();
                    } else if ("title".equals(this.streamReader.getLocalName())) {
                        inTitle = true;
                    } else if ("abstract".equals(this.streamReader.getLocalName())) {
                        inText = true;
                    } else if ("url".equals(this.streamReader.getLocalName())) {
                        inUrl = true;
                    }
                } else if (this.streamReader.getEventType() == XMLStreamConstants.CHARACTERS) {
                    if (inUrl) {
                        currentDoc.setUrl(this.streamReader.getText());
                    } else if (inText) {
                        currentDoc.setText(this.streamReader.getText());
                    } else if (inTitle) {
                        currentDoc.setTitle(this.streamReader.getText());
                    }
                } else if(this.streamReader.getEventType() == XMLStreamReader.END_ELEMENT){
                    if ("doc".equals(this.streamReader.getLocalName())) {
                        if (parsed % every == 0) {
                            documents.add(currentDoc);
                        }
                        parsed++;
                        this.xmlMeter.mark();
                        if (this.xmlMeter.getCount() % 50000 == 0) {
                            LOG.info("Parsed {} documents, current rate {} docs/sec.",
                                    this.xmlMeter.getCount(),
                                    this.xmlMeter.getOneMinuteRate());
                        }
                        currentDoc = null;
                    } else if ("title".equals(this.streamReader.getLocalName())) {
                        inTitle = false;
                    } else if ("abstract".equals(this.streamReader.getLocalName())) {
                        inText = false;
                    } else if ("url".equals(this.streamReader.getLocalName())) {
                        inUrl = false;
                    }
                }
            }
        } catch (XMLStreamException e) {
            LOG.error("Cannot parse xml.", e);
        }
        return documents;
    }

    public boolean isRead() {
        try {
            return !this.streamReader.hasNext();
        } catch (XMLStreamException e) {
            return false;
        }
    }

}
