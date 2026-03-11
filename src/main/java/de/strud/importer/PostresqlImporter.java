package de.strud.importer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.strud.data.Document;
import de.strud.exceptions.DBImporterInitializationException;

/**
 * A Postresql specific implementation, that inserts the articles. The database 'evaluation' needs to be
 * created before starting the import and the grants for the user root need to be set to any ip.
 *
 * User: strud
 */
public class PostresqlImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(PostresqlImporter.class);
    private static final String CONNECTION_TEMPLATE = "jdbc:postgresql://%s:%d/evaluation?user=root";
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS articles";
    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE articles (
              url varchar(255) PRIMARY KEY,
              title text,
              text text
            )
            """;
    private static final String INSERT_SQL = "INSERT INTO articles (url, title, text) VALUES (?, ?, ?)";

    private final Connection connection;

    public PostresqlImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            this.connection = DriverManager.getConnection(CONNECTION_TEMPLATE.formatted(host, port));
            try (PreparedStatement clean = this.connection.prepareStatement(DROP_TABLE_SQL)) {
                clean.execute();
            } catch (SQLException inner) {
                LOG.error("Cannot cleanup old table.", inner);
            }

            try (PreparedStatement create = this.connection.prepareStatement(CREATE_TABLE_SQL)) {
                create.execute();
            } catch (SQLException inner) {
                LOG.error("Cannot create empty table.", inner);
            }
        } catch (SQLException e) {
            throw new DBImporterInitializationException("Cannot connect to postgresql server.", e);
        }
    }

    @Override
    public boolean importDocument(Document document) {
        try (PreparedStatement insert = this.connection.prepareStatement(INSERT_SQL)) {
            insert.setString(1, document.getUrl());
            insert.setString(2, document.getTitle());
            insert.setString(3, document.getText());
            insert.execute();
        } catch (SQLException e) {
            LOG.error("Cannot import document.", e);
            return false;
        }
        return true;
    }
}
