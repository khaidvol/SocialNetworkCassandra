package model;

import java.util.Date;

public class Movie {

  private long id;
  private String title;
  private String country;
  private Date year;

  public Movie() {}

  public Movie(long id, String title, String country, Date year) {
    this.id = id;
    this.title = title;
    this.country = country;
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

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public Date getYear() {
    return year;
  }

  public void setYear(Date year) {
    this.year = year;
  }

  @Override
  public String toString() {
    return "Movie{"
        + "id="
        + id
        + ", title='"
        + title
        + '\''
        + ", country='"
        + country
        + '\''
        + ", year="
        + year
        + '}';
  }
}
