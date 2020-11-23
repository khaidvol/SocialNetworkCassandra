package connection;

import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

public class DatabaseCreator {

  private static final Logger LOGGER = Logger.getLogger(DatabaseCreator.class);
  private static final String KEYSPACE_NAME = "sntask5";
  private static final String REPLICATION_STRATEGY = "SimpleStrategy";
  private static final int REPLICATION_FACTOR = 1;

  private static final String TABLE_NAME_MOVIES = "sntask5.movies";
  private static final String TABLE_NAME_AUDIO_TRACKS = "sntask5.audioTracks";
  private static final String TABLE_NAME_USERS = "sntask5.users";
  private static final String TABLE_NAME_FRIENDSHIPS = "sntask5.friendships";
  private static final String TABLE_NAME_MESSAGES = "sntask5.messages";
  public static final String TABLE_CREATED = "Table created - ";
  public static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS ";
  public static final String CREATE_ID_CQL = "id bigint PRIMARY KEY, ";

  private DatabaseCreator() {
  }

  public static void createKeyspaceWithTables(){
    dropKeyspace(KEYSPACE_NAME);
    createKeyspace(KEYSPACE_NAME, REPLICATION_STRATEGY, REPLICATION_FACTOR);
    createTableMovies();
    createTableAudioTracks();
    createTableUsers();
    createTableFriendships();
    createTableMessages();
  }

  public static void createKeyspace(
      String keyspaceName, String replicationStrategy, int replicationFactor) {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
            .append(keyspaceName)
            .append(" WITH replication = {")
            .append("'class':'")
            .append(replicationStrategy)
            .append("','replication_factor':")
            .append(replicationFactor)
            .append("};");

    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info("Keyspace created - " + keyspaceName);
  }

  public static void dropKeyspace(String keyspaceName) {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder("DROP KEYSPACE IF EXISTS ").append(keyspaceName).append(";");

    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info("Keyspace dropped - " + keyspaceName);
  }

  public static void createTableMovies() {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder(CREATE_TABLE_IF_NOT_EXISTS)
            .append(TABLE_NAME_MOVIES)
            .append("(")
            .append(CREATE_ID_CQL)
            .append("title text,")
            .append("country text,")
            .append("year date);");

    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info(TABLE_CREATED + TABLE_NAME_MOVIES);
  }

  public static void createTableAudioTracks() {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder(CREATE_TABLE_IF_NOT_EXISTS)
            .append(TABLE_NAME_AUDIO_TRACKS)
            .append("(")
            .append(CREATE_ID_CQL)
            .append("title text,")
            .append("author text,")
            .append("album text,")
            .append("year date);");
    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info(TABLE_CREATED + TABLE_NAME_AUDIO_TRACKS);
  }

  public static void createTableUsers() {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder(CREATE_TABLE_IF_NOT_EXISTS)
            .append(TABLE_NAME_USERS)
            .append("(")
            .append(CREATE_ID_CQL)
            .append("name text,")
            .append("surname text,")
            .append("birthdate date,")
            .append("movies list<bigint>,")
            .append("audioTracks list<bigint>);");
    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();

    LOGGER.info(TABLE_CREATED + TABLE_NAME_USERS);
  }

  public static void createTableFriendships() {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder(CREATE_TABLE_IF_NOT_EXISTS)
            .append(TABLE_NAME_FRIENDSHIPS)
            .append("(")
            .append("userid bigint PRIMARY KEY,")
            .append("friendsIds list<bigint>,")
            .append("date date);");
    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info(TABLE_CREATED + TABLE_NAME_FRIENDSHIPS);
  }

  public static void createTableMessages() {
    Session session = CassandraConnection.getSession();
    StringBuilder sb =
        new StringBuilder(CREATE_TABLE_IF_NOT_EXISTS)
            .append(TABLE_NAME_MESSAGES)
            .append("(")
            .append("id bigint PRIMARY KEY,")
            .append("senderId bigint,")
            .append("recipientId bigint,")
            .append("text text,")
            .append("date date);");
    String query = sb.toString();
    session.execute(query);
    CassandraConnection.closeSession();
    LOGGER.info(TABLE_CREATED + TABLE_NAME_MESSAGES);
  }
}
