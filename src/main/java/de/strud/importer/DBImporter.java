package de.strud.importer;

import de.strud.data.Document;

/**
 * Implementations of this interface should map and store documents in an specific db.
 *
 * User: strud
 */
public interface DBImporter {

    boolean importDocument(Document document);

}
