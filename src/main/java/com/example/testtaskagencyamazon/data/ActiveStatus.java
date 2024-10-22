package com.example.testtaskagencyamazon.data;

import java.io.Serializable;
import java.util.List;

public enum ActiveStatus implements Serializable {

  ENABLED, PAUSED, ARCHIVED;

  public static ActiveStatus fromString(String status) {
    if (status == null) return null;

    return switch (status.toUpperCase()) {
      case "ENABLED" -> ENABLED;
      case "PAUSED" -> PAUSED;
      case "ARCHIVED" -> ARCHIVED;
      default -> null;
    };
  }

  public List<ActiveStatus> toList() {
    return List.of(this);
  }

  public boolean equals(String status) {
    if (status == null) return false;
    return this.name().equalsIgnoreCase(status.toUpperCase());
  }

}