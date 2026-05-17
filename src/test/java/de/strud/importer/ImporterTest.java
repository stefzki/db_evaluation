package de.strud.importer;

import de.strud.data.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImporterTest {

    @Test
    void importsAllDocumentsSuccessfully() {
        RecordingDBImporter dbImporter = new RecordingDBImporter(List.of(true, true));
        Document first = new Document("https://example.test/1", "First", "First text");
        Document second = new Document("https://example.test/2", "Second", "Second text");

        Importer importer = new Importer(dbImporter);

        assertTrue(importer.importDocuments(List.of(first, second)));
        assertEquals(List.of(first, second), dbImporter.importedDocuments);
    }

    @Test
    void returnsFalseWhenAnyDocumentImportFails() {
        RecordingDBImporter dbImporter = new RecordingDBImporter(List.of(true, false));
        Document successful = new Document("https://example.test/success", "Success", "Success text");
        Document failed = new Document("https://example.test/failure", "Failure", "Failure text");

        Importer importer = new Importer(dbImporter);

        assertFalse(importer.importDocuments(List.of(successful, failed)));
        assertEquals(List.of(successful, failed), dbImporter.importedDocuments);
    }

    @Test
    void returnsTrueForEmptyDocumentList() {
        RecordingDBImporter dbImporter = new RecordingDBImporter(List.of());

        Importer importer = new Importer(dbImporter);

        assertTrue(importer.importDocuments(List.of()));
        assertTrue(dbImporter.importedDocuments.isEmpty());
    }

    private static class RecordingDBImporter implements DBImporter {

        private final List<Boolean> results;

        private final List<Document> importedDocuments = new ArrayList<>();

        private RecordingDBImporter(final List<Boolean> results) {
            this.results = results;
        }

        @Override
        public boolean importDocument(final Document document) {
            this.importedDocuments.add(document);
            return this.results.get(this.importedDocuments.size() - 1);
        }
    }
}
