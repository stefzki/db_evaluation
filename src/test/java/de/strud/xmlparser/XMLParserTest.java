package de.strud.xmlparser;

import de.strud.data.Document;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: strud
 */
public class XMLParserTest {

    @Test
    public void testParsing() throws Exception {
        XMLParser parser = new XMLParser(
                this.getClass().getClassLoader().getResourceAsStream("enwiki_short.xml"), 1);
        assertTrue(!parser.isRead(), "Parser should have entries.");
        List<Document> docs = parser.parse(1);
        assertNotNull(docs, "Documents cannot be null.");
        assertTrue(docs.size() > 0, "Documents should have length > 0.");
        assertNotNull(docs.get(0).getText(), "Text cannot be null.");
        assertNotNull(docs.get(0).getTitle(), "Title cannot be null.");
        assertNotNull(docs.get(0).getUrl(), "URL cannot be null.");
    }

    @Test
    public void testParsingWithNull() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new XMLParser(null, 0));
    }
}
