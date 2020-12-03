package connection;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import model.*;
import org.apache.log4j.Logger;

import java.util.List;

public class DataLoader {

  private static final Logger LOGGER = Logger.getLogger(DataLoader.class);

  private static final String BATCH_START = "Storing batch #%s/%s to the CassandraDB...";
  private static final String BATCH_FINISH = "Batch #%s/%s stored successfully!";
  private static final String DELIMITER = "-----------------------------------------";

  private static final String INSERT_USER_CQL =
      "insert into sntask5.users (id, name, surname, birthdate, audioTracks, movies) values (?, ?, ?, ?, ?, ?)";
  private static final String INSERT_MOVIE_CQL =
      "insert into sntask5.movies (id, title, country, year) values (?, ?, ?, ?)";
  private static final String INSERT_AUDIO_TRACK_CQL =
      "insert into sntask5.audioTracks (id, title, author, album, year) values (?, ?, ?, ?, ?)";
  private static final String INSERT_MESSAGE_CQL =
      "insert into sntask5.messages (id, senderid, recipientid, text, date) values (?, ?, ?, ?, ?)";
  private static final String INSERT_FRIENDSHIP_CQL =
      "insert into sntask5.friendships (userId, friendsIds, date) values (?, ?, ?)";

  public static final long BATCH_SIZE = 100_000;

  public static final long NUMBER_OF_USERS = 100_000;
  public static final long NUMBER_OF_MOVIES = 1_000_000;
  public static final long NUMBER_AUDIO_TRACKS = 1_000_000;
  public static final long NUMBER_MESSAGES = 1_000_000;
  public static final long NUMBER_FRIENDSHIPS = 1_000_000;

  private DataLoader() {}

  public static void executeLoading() {
    loadMovies(BATCH_SIZE, NUMBER_OF_MOVIES);
    loadAudioTracks(BATCH_SIZE, NUMBER_AUDIO_TRACKS);
    loadUsers(BATCH_SIZE, NUMBER_OF_USERS, NUMBER_OF_MOVIES, NUMBER_AUDIO_TRACKS);
    loadFriendships(BATCH_SIZE, NUMBER_FRIENDSHIPS, NUMBER_OF_USERS);
    loadMessages(BATCH_SIZE, NUMBER_MESSAGES, NUMBER_OF_USERS);
  }

  public static void loadUsers(
      long batchSize, long numberOfUsers, long maxNumberOfMovies, long maxNumberOfAudioTracks) {

    Session session = CassandraConnection.getSession();
    PreparedStatement preparedStatement = session.prepare(INSERT_USER_CQL);

    long numberOfBatches = numberOfUsers / batchSize;
    long startUsersId = 0;
    long endUsersId = batchSize;

    for (long i = 0; i < numberOfBatches; i++) {

      List<User> users =
          DataGenerator.generateUsers(
              startUsersId, endUsersId, maxNumberOfMovies, maxNumberOfAudioTracks);
      LOGGER.info(String.format(BATCH_START, i + 1, numberOfBatches));

      users.forEach(
          user ->
              session.execute(
                  preparedStatement.bind(
                      user.getId(),
                      user.getName(),
                      user.getSurname(),
                      LocalDate.fromMillisSinceEpoch(
                          user.getBirthdate().toInstant().toEpochMilli()),
                      user.getAudioTracks(),
                      user.getMovies())));

      LOGGER.info(String.format(BATCH_FINISH, i + 1, numberOfBatches));
      LOGGER.info(DELIMITER);

      startUsersId = endUsersId;
      endUsersId += batchSize;
    }

    CassandraConnection.closeSession();
  }

  public static void loadMovies(long batchSize, long numberOfMovies) {

    Session session = CassandraConnection.getSession();
    PreparedStatement preparedStatement = session.prepare(INSERT_MOVIE_CQL);

    long numberOfBatches = numberOfMovies / batchSize;
    long startMoviesId = 0;
    long endMoviesId = batchSize;

    for (long i = 0; i < numberOfBatches; i++) {

      List<Movie> movies = DataGenerator.generateMovies(startMoviesId, endMoviesId);

      LOGGER.info(String.format(BATCH_START, i + 1, numberOfBatches));
      movies.forEach(
          movie ->
              session.execute(
                  preparedStatement.bind(
                      movie.getId(),
                      movie.getTitle(),
                      movie.getCountry(),
                      LocalDate.fromMillisSinceEpoch(movie.getYear().toInstant().toEpochMilli()))));

      LOGGER.info(String.format(BATCH_FINISH, i + 1, numberOfBatches));
      LOGGER.info(DELIMITER);

      startMoviesId = endMoviesId;
      endMoviesId += batchSize;
    }
    CassandraConnection.closeSession();
  }

  public static void loadAudioTracks(long batchSize, long numberOfAudioTracks) {

    Session session = CassandraConnection.getSession();
    PreparedStatement preparedStatement = session.prepare(INSERT_AUDIO_TRACK_CQL);

    long numberOfBatches = numberOfAudioTracks / batchSize;
    long startAudioTracksId = 0;
    long endAudioTracksId = batchSize;

    for (long i = 0; i < numberOfBatches; i++) {
      List<AudioTrack> audioTracks =
          DataGenerator.generateAudioTracks(startAudioTracksId, endAudioTracksId);

      LOGGER.info(String.format(BATCH_START, i + 1, numberOfBatches));
      audioTracks.forEach(
          audioTrack ->
              session.execute(
                  preparedStatement.bind(
                      audioTrack.getId(),
                      audioTrack.getTitle(),
                      audioTrack.getAuthor(),
                      audioTrack.getAlbum(),
                      LocalDate.fromMillisSinceEpoch(
                          audioTrack.getYear().toInstant().toEpochMilli()))));

      LOGGER.info(String.format(BATCH_FINISH, i + 1, numberOfBatches));
      LOGGER.info(DELIMITER);

      startAudioTracksId = endAudioTracksId;
      endAudioTracksId += batchSize;
    }
    CassandraConnection.closeSession();
  }

  public static void loadMessages(long batchSize, long numberOfMessages, long maxNumberOfUsers) {
    Session session = CassandraConnection.getSession();
    PreparedStatement preparedStatement = session.prepare(INSERT_MESSAGE_CQL);

    long startMessagesId = 0;
    long endMessagesId = batchSize;

    long numberOfBatches = numberOfMessages / batchSize;
    for (long i = 0; i < numberOfBatches; i++) {

      List<Message> messages =
          DataGenerator.generateMessages(startMessagesId, endMessagesId, maxNumberOfUsers);
      LOGGER.info(String.format(BATCH_START, i + 1, numberOfBatches));

      messages.forEach(
          message ->
              session.execute(
                  preparedStatement.bind(
                      message.getId(),
                      message.getSenderId(),
                      message.getRecipientId(),
                      message.getText(),
                      LocalDate.fromMillisSinceEpoch(
                          message.getDate().toInstant().toEpochMilli()))));

      LOGGER.info(String.format(BATCH_FINISH, i + 1, numberOfBatches));
      LOGGER.info(DELIMITER);

      startMessagesId = endMessagesId;
      endMessagesId += batchSize;
    }

    CassandraConnection.closeSession();
  }

  public static void loadFriendships(
      long batchSize, long numberOfFriendships, long maxNumberOfUsers) {
    Session session = CassandraConnection.getSession();
    PreparedStatement preparedStatement = session.prepare(INSERT_FRIENDSHIP_CQL);

    long startFriendshipsId = 0;
    long endFriendshipsId = batchSize;

    long numberOfBatches = numberOfFriendships / batchSize;
    for (long i = 0; i < numberOfBatches; i++) {
      List<Friendship> friendships =
          DataGenerator.generateFriendships(startFriendshipsId, endFriendshipsId, maxNumberOfUsers);
      LOGGER.info(String.format(BATCH_START, i + 1, numberOfBatches));

      friendships.forEach(
          friendship ->
              session.execute(
                  preparedStatement.bind(
                      friendship.getUserId(),
                      friendship.getFriendsIds(),
                      LocalDate.fromMillisSinceEpoch(
                          friendship.getDate().toInstant().toEpochMilli()))));

      LOGGER.info(String.format(BATCH_FINISH, i + 1, numberOfBatches));
      LOGGER.info(DELIMITER);

      startFriendshipsId = endFriendshipsId;
      endFriendshipsId += batchSize;
    }

    CassandraConnection.closeSession();
  }
}
