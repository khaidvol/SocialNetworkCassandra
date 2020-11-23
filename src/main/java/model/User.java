package model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class User {

  private long id;
  private String name;
  private String surname;
  private Date birthdate;
  private List<Long> audioTracks;
  private List<Long> movies;

  public User() {
    audioTracks = new ArrayList<>();
    movies = new ArrayList<>();
  }

  public User(
      long id,
      String name,
      String surname,
      Date birthdate,
      List<Long> audioTracks,
      List<Long> movies) {
    this.id = id;
    this.name = name;
    this.surname = surname;
    this.birthdate = birthdate;
    this.audioTracks = audioTracks;
    this.movies = movies;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSurname() {
    return surname;
  }

  public void setSurname(String surname) {
    this.surname = surname;
  }

  public Date getBirthdate() {
    return birthdate;
  }

  public void setBirthdate(Date birthdate) {
    this.birthdate = birthdate;
  }

  public List<Long> getAudioTracks() {
    return audioTracks;
  }

  public void setAudioTracks(List<Long> audioTracks) {
    this.audioTracks = audioTracks;
  }

  public List<Long> getMovies() {
    return movies;
  }

  public void setMovies(List<Long> movies) {
    this.movies = movies;
  }

  @Override
  public String toString() {
    return "User{"
        + "id="
        + id
        + ", name='"
        + name
        + ", surname='"
        + surname
        + ", birthdate="
        + birthdate
        + ", audioTracks="
        + audioTracks
        + ", movies="
        + movies
        + '}';
  }
}
