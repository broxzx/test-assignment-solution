package com.example.testtaskagencyamazon.data;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@Builder
@EqualsAndHashCode(of = {"campaignId", "name", "state"})
public class SPCampaign implements Serializable {

  private String campaignId;

  private String portfolioId;

  @DateTimeFormat(pattern = "yyyy-MM-dd")
  private String startDate;

  @DateTimeFormat(pattern = "yyyy-MM-dd")
  private String endDate;

  private String name;

  private String state;

  private String targetingType;

}
