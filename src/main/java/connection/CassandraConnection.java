package connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CassandraConnection {

  private static final Logger LOGGER = Logger.getLogger(CassandraConnection.class);
  private static Cluster cluster;
  private static Session session;

  private CassandraConnection() {
  }

  public static Session getSession() {
    Properties propCassandra = new Properties();
    try (InputStream input = new FileInputStream("src/main/resources/cassandra.properties")) {
      propCassandra.load(input);
    } catch (IOException e) {
      LOGGER.error(e);
    }

    String node = propCassandra.getProperty("cassandra.node");
    int port = Integer.parseInt(propCassandra.getProperty("cassandra.port"));

    cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
    session = cluster.connect();
    return session;
  }

  public static void closeSession() {
    session.close();
    cluster.close();
  }
}
