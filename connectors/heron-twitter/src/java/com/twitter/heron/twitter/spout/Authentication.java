package com.streamlio.connectors.twitter;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Authentication {
  private String   consumerKey;
  private String   consumerSecret;
  private String   accessToken;
  private String   accessTokenSecret;
  private String[] keyWords;

  public Authentication() {
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public void setConsumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

  public void setConsumerSecret(String consumerSecret) {
    this.consumerSecret = consumerSecret;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getAccessTokenSecret() {
    return accessTokenSecret;
  }

  public void setAccessTokenSecret(String accessTokenSecret) {
    this.accessTokenSecret = accessTokenSecret;
  }

  public String[] getKeyWords() {
    return keyWords;
  }

  public void setKeyWords(String[] keyWords) {
    this.keyWords = keyWords;
  }
}
