package com.example.testtaskagencyamazon.data;

import java.io.Serializable;

public enum DirectionType implements Serializable {

  ASC("ASC"),
  DESC("DESC");

  DirectionType(String desc) {
  }

  public static DirectionType getDirection(String direction) {
    return switch (direction) {
      case "ASC" -> ASC;
      case "DESC" -> DESC;
      default -> throw new IllegalArgumentException("Wrong argument");
    };
  }
}
