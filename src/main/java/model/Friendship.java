package model;

import java.util.Date;
import java.util.List;

public class Friendship {

  private long userId;
  private List<Long> friendsIds;
  private Date date;

  public Friendship() {}

  public Friendship(long userId, List<Long> friendsIds, Date date) {
    this.userId = userId;
    this.friendsIds = friendsIds;
    this.date = date;
  }

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public List<Long> getFriendsIds() {
    return friendsIds;
  }

  public void setFriendsIds(List<Long> friendsIds) {
    this.friendsIds = friendsIds;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }
}
