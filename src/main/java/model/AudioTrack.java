package model;

import java.util.Date;

public class AudioTrack {

  private long id;
  private String title;
  private String author;
  private String album;
  private Date year;

  public AudioTrack() {}

  public AudioTrack(long id, String title, String author, String album, Date year) {
    this.id = id;
    this.title = title;
    this.author = author;
    this.album = album;
    this.year = year;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getAlbum() {
    return album;
  }

  public void setAlbum(String album) {
    this.album = album;
  }

  public Date getYear() {
    return year;
  }

  public void setYear(Date year) {
    this.year = year;
  }
}
