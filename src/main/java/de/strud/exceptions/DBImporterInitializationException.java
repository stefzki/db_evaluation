package de.strud.exceptions;

/**
 * This exception will be thrown by all importer implementations, when connection problems occur.
 *
 * User: strud
 */
public class DBImporterInitializationException extends Exception {

    public DBImporterInitializationException(final String message) {
        super(message);
    }

    public DBImporterInitializationException(final String message, final Exception cause) {
        super(message, cause);
    }

}
