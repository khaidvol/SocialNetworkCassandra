import connection.DataLoader;
import connection.DatabaseCreator;
import logic.ActivityReport;

public class SocialNetworkApp {

  public static void main(String[] args) {

    DatabaseCreator.createKeyspaceWithTables();
    DataLoader.executeLoading();

    ActivityReport.showAverageMessagesByDayOfWeek();
    ActivityReport.showMaxNumberOfNewFriendshipsFromMonthToMonth();
    ActivityReport.showMinNumberOfWatchedMoviesByUsersWithMoreThan100friends();
  }
}
