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
 * A MySQL specific implementation, that inserts the articles into a innodb table. The database 'wikipedia' needs to be
 * created before starting the import and the grants for the user root need to be set to any ip.
 *
 * User: strud
 */
public class MysqlImporter implements DBImporter {

    private static final Logger LOG = LogManager.getLogger(MysqlImporter.class);

    private final Connection connection;

    public MysqlImporter(final String host, final int port) throws DBImporterInitializationException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/?user=root");
            try (PreparedStatement clean = this.connection.prepareStatement("DROP TABLE IF EXISTS `evaluation`.`articles`") ) {
                clean.execute();
            } catch (SQLException inner) {
                LOG.error("Cannot cleanup old table.", inner);
            }

            try (PreparedStatement create = this.connection.prepareStatement("CREATE TABLE `evaluation`.`articles` (\n" +
                    "  `url` varchar(255),\n" +
                    "  `title` text,\n" +
                    "  `text` text,\n" +
                    "  INDEX `url_idx` (`url`)\n" +
                    ") ENGINE=InnoDB") ) {
                create.execute();
            } catch (SQLException inner) {
                LOG.error("Cannot create empty table.", inner);
            }
        } catch (ClassNotFoundException | SQLException e) {
            throw new DBImporterInitializationException("Cannot connect to mysql server.", e);
        }
    }

    @Override
    public boolean importDocument(Document document) {
        try (PreparedStatement insert = this.connection.prepareStatement("INSERT INTO `evaluation`.`articles` SET url=?, title=?, text=?") ) {
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
