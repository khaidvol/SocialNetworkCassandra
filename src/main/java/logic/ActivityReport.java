package logic;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import connection.CassandraConnection;
import org.apache.log4j.Logger;

import java.util.*;

public class ActivityReport {

  public static final Logger LOGGER = Logger.getLogger(ActivityReport.class);

  private static final String SELECT_MESSAGES_DATE_SQL = "SELECT date FROM sntask5.messages";
  private static final String SELECT_FRIENDSHIPS_SQL =
      "SELECT userId, friendsIds, date FROM sntask5.friendships";
  private static final String SELECT_USERS_SQL = "SELECT id, movies FROM sntask5.users";

  private ActivityReport() {}

  public static void showAverageMessagesByDayOfWeek() {
    try (Session session = CassandraConnection.getSession()) {
      Calendar c = Calendar.getInstance();
      Map<LocalDate, Integer> tempMap = new HashMap<>();
      Map<Integer, List<Integer>> convertingMap = new HashMap<>();
      convertingMap.put(1, new ArrayList<>());
      convertingMap.put(2, new ArrayList<>());
      convertingMap.put(3, new ArrayList<>());
      convertingMap.put(4, new ArrayList<>());
      convertingMap.put(5, new ArrayList<>());
      convertingMap.put(6, new ArrayList<>());
      convertingMap.put(7, new ArrayList<>());

      PreparedStatement preparedStatement = session.prepare(SELECT_MESSAGES_DATE_SQL);
      ResultSet resultSet = session.execute(preparedStatement.bind());

      resultSet.forEach(
          row -> {
            LocalDate localDate = row.getDate(0);
            Integer frequencyCounter = tempMap.get(localDate);
            tempMap.put(localDate, frequencyCounter == null ? 1 : frequencyCounter + 1);
          });

      for (Map.Entry<LocalDate, Integer> entry : tempMap.entrySet()) {
        LocalDate localDate = entry.getKey();
        c.setTimeInMillis(localDate.getMillisSinceEpoch());
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
        convertingMap.get(dayOfWeek).add(entry.getValue());
      }

      for (Map.Entry<Integer, List<Integer>> entry : convertingMap.entrySet()) {
        LOGGER.info(
            "Average number of messages by day of week -"
                + " Day of the week: "
                + entry.getKey()
                + ", Average number of messages: "
                + entry.getValue().stream().mapToDouble(d -> d).average().getAsDouble());
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    CassandraConnection.closeSession();
  }

  public static void showMaxNumberOfNewFriendshipsFromMonthToMonth() {

    try (Session session = CassandraConnection.getSession()) {
      PreparedStatement preparedStatement = session.prepare(SELECT_FRIENDSHIPS_SQL);
      ResultSet resultSet = session.execute(preparedStatement.bind());
      Calendar c = Calendar.getInstance();
      Map<String, Integer> tempMap = new TreeMap<>();

      resultSet.forEach(
          row -> {
            LocalDate localDate = row.getDate(2);
            Integer friends = row.getList(1, Long.class).size();
            c.setTimeInMillis(localDate.getMillisSinceEpoch());
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH);
            String strDate = year + "-" + month;
            Integer current = tempMap.get(strDate);
            if (current == null || current < friends) {
              tempMap.put(strDate, friends);
            }
          });

      tempMap.forEach(
          (key, value) ->
              LOGGER.info(
                  "Max number of new friendships from month to month -"
                      + " yyyy-MM: "
                      + key
                      + ", Max number of new friendships: "
                      + value));
    } catch (Exception e) {
      LOGGER.error(e);
    }
    CassandraConnection.closeSession();
  }

  public static void showMinNumberOfWatchedMoviesByUsersWithMoreThan100friends() {
    try (Session session = CassandraConnection.getSession()) {
      PreparedStatement preparedStatementUser = session.prepare(SELECT_USERS_SQL);
      PreparedStatement preparedStatementFriendships = session.prepare(SELECT_FRIENDSHIPS_SQL);
      ResultSet resultSetUsers = session.execute(preparedStatementUser.bind());
      ResultSet resultSetFriends = session.execute(preparedStatementFriendships.bind());

      Map<Long, Integer> friends = new TreeMap<>();
      Map<Long, Integer> users = new TreeMap<>();

      resultSetFriends.forEach(
          row -> {
            long id = row.getLong(0);
            int numberOfFriends = row.getList(1, Long.class).size();
            friends.put(id, numberOfFriends);
          });

      resultSetUsers.forEach(
          row -> {
            long id = row.getLong(0);
            int numberOfMovies = row.getList(1, Long.class).size();
            if (friends.get(id) > 100) {
              users.put(id, numberOfMovies);
            }
          });

      users.entrySet().stream()
          .sorted(Map.Entry.comparingByValue(Comparator.naturalOrder()))
          .limit(500)
          .forEachOrdered(
              entry ->
                  LOGGER.info(
                      "Min number of watched movies by users with more than 100 friends -"
                          + " User Id: "
                          + entry.getKey()
                          + ", Min number of movies: "
                          + entry.getValue()));

    } catch (Exception e) {
      LOGGER.error(e);
    }
    CassandraConnection.closeSession();
  }
}
