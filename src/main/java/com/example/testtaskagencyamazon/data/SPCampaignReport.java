package com.example.testtaskagencyamazon.data;

import java.io.Serializable;
import java.math.BigDecimal;
import lombok.Data;
import org.bson.types.ObjectId;

@Data
public class SPCampaignReport implements Serializable {

  private ObjectId reportId;
  private Long profileId;
  private String portfolioId;
  private Long campaignId;
  private String campaignName;
  private String campaignStatus;
  private Integer clicks;
  private BigDecimal cost;
  private BigDecimal costPerClick;
  private String date;
  private Integer impressions;
  private Integer purchases1d;
  private Integer purchases14d;
  private Integer purchasesSameSku1d;
  private Integer purchasesSameSku14d;
  private BigDecimal sales1d;
  private BigDecimal sales14d;
  private BigDecimal spend;

}