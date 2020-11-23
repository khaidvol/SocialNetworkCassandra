package connection;

import model.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DataGenerator {

  public static final String SUCCESS = "List generated successfully!";
  private static final Logger LOGGER = Logger.getLogger(DataGenerator.class);
  private static Random rm = new Random();

  private DataGenerator() {}

  public static List<User> generateUsers(
      long startUsersId, long endUsersId, long maxMovieId, long maxAudioTrackId) {
    LOGGER.info("Generating list of users...");
    List<User> users = new ArrayList<>();

    for (long i = startUsersId; i < endUsersId; i++) {

      User user = new User();
      user.setId(i);
      user.setName(generateRandomString());
      user.setSurname(generateRandomString());
      user.setBirthdate(generateRandomDate(1920, 2002));
      user.setMovies(generateRandomLongs(new Random().nextInt(500), maxMovieId));
      user.setAudioTracks(generateRandomLongs(new Random().nextInt(1000), maxAudioTrackId));
      users.add(user);
    }

    LOGGER.info(SUCCESS);
    return users;
  }

  public static List<Message> generateMessages(
      long startMessagesId, long endMessagesId, long maxUserId) {
    LOGGER.info("Generating list of messages...");
    List<Message> messages = new ArrayList<>();

    List<Long> receiverAndRecipient = generateRandomLongs(2, maxUserId);

    for (long i = startMessagesId; i < endMessagesId; i++) {

      Message message = new Message();
      message.setId(i);
      message.setSenderId(receiverAndRecipient.get(0));
      message.setRecipientId(receiverAndRecipient.get(1));
      message.setText(generateRandomString());
      message.setDate(generateRandomDate(2015, 2020));
      messages.add(message);
    }

    LOGGER.info(SUCCESS);
    return messages;
  }

  public static List<Movie> generateMovies(long startMoviesId, long endMoviesId) {
    LOGGER.info("Generating list of movies...");
    List<Movie> movies = new ArrayList<>();

    for (long i = startMoviesId; i < endMoviesId; i++) {

      Movie movie = new Movie();
      movie.setId(i);
      movie.setTitle(generateRandomString());
      movie.setCountry(generateRandomString());
      movie.setYear(generateRandomDate(1930, 2020));
      movies.add(movie);
    }

    LOGGER.info(SUCCESS);
    return movies;
  }

  public static List<AudioTrack> generateAudioTracks(
      long startAudioTracksId, long endAudioTracksId) {
    LOGGER.info("Generating list of audio tracks...");
    List<AudioTrack> audioTracks = new ArrayList<>();

    for (long i = startAudioTracksId; i < endAudioTracksId; i++) {

      AudioTrack audioTrack = new AudioTrack();
      audioTrack.setId(i);
      audioTrack.setTitle(generateRandomString());
      audioTrack.setAuthor(generateRandomString());
      audioTrack.setAlbum(generateRandomString());
      audioTrack.setYear(generateRandomDate(1950, 2020));
      audioTracks.add(audioTrack);
    }

    LOGGER.info(SUCCESS);
    return audioTracks;
  }

  public static List<Friendship> generateFriendships(
      long startFriendshipsId, long endFriendshipsId, long maxUsersId) {
    LOGGER.info("Generating list of friendships...");
    List<Friendship> friendships = new ArrayList<>();

    for (long i = startFriendshipsId; i < endFriendshipsId; i++) {
      Friendship friendship = new Friendship();
      friendship.setUserId(i);
      friendship.setFriendsIds(generateRandomLongs(new Random().nextInt(500), maxUsersId));
      friendship.setDate(generateRandomDate(2015, 2020));
      friendships.add(friendship);
    }

    LOGGER.info(SUCCESS);
    return friendships;
  }

  /** Generators for words, numbers, dates */
  private static String generateRandomString() {
    String randomString;
    String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    StringBuilder sb = new StringBuilder();
    int lengthOfBuiltString = rm.nextInt(20 - 3) + 3;
    for (int n = 0; n < lengthOfBuiltString; n++) {
      int index = rm.nextInt(alphabet.length());
      char randomChar = alphabet.charAt(index);
      sb.append(randomChar);
    }
    randomString = sb.toString();
    return randomString;
  }

  private static List<Long> generateRandomLongs(int amount, long max) {
    Set<Long> set = new HashSet<>();
    while (set.size() < amount) {
      set.add(ThreadLocalRandom.current().nextLong(1, max));
    }
    return new ArrayList<>(set);
  }

  private static Date generateRandomDate(int fromDate, int toDate) {
    long aDay = TimeUnit.DAYS.toMillis(1);
    long now = new Date().getTime();
    long start = new Date(now - aDay * 365 * (2020 - fromDate)).getTime();
    long end = new Date(now - aDay * 365 * (2020 - toDate)).getTime();
    return new Date(ThreadLocalRandom.current().nextLong(start, end));
  }
}
