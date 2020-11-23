package model;

import java.util.Date;

public class Message {

  private long id;
  private long senderId;
  private long recipientId;
  private String text;
  private Date date;

  public Message() {}

  public Message(long id, long senderId, long recipientId, String text, Date date) {
    this.id = id;
    this.senderId = senderId;
    this.recipientId = recipientId;
    this.text = text;
    this.date = date;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getSenderId() {
    return senderId;
  }

  public void setSenderId(long senderId) {
    this.senderId = senderId;
  }

  public long getRecipientId() {
    return recipientId;
  }

  public void setRecipientId(long recipientId) {
    this.recipientId = recipientId;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }
}
