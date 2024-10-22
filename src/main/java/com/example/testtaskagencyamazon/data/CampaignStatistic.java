package com.example.testtaskagencyamazon.data;

import static com.example.testtaskagencyamazon.utils.MathUtil.calculateACoS;
import static com.example.testtaskagencyamazon.utils.MathUtil.calculateCPC;
import static com.example.testtaskagencyamazon.utils.MathUtil.calculateCTR;
import static com.example.testtaskagencyamazon.utils.MathUtil.calculateConversion;
import static com.example.testtaskagencyamazon.utils.MathUtil.finalRound;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public abstract class CampaignStatistic<T extends CampaignStatistic<T>> implements Serializable, Analytic<T> {

  @JsonIgnore
  protected Long profileId;

  @JsonIgnore
  protected Long portfolioId;

  @JsonIgnore
  protected String date;

  protected Long campaignId;

  protected String campaignName;

  protected String state;

  protected BigDecimal advertisingCostOfSales;

  protected BigDecimal clickThroughRate;

  protected BigDecimal costPerClick;

  protected BigDecimal conversion;

  protected Integer clicks;

  protected BigDecimal cost;

  protected Integer impressions;

  protected BigDecimal sales;

  protected Integer purchases;

  @Getter(AccessLevel.PROTECTED)
  @Setter(AccessLevel.NONE)
  private Integer counter = 1;

  public CampaignStatistic(Long profileId, Long portfolioId,
      String date, Long campaignId, String campaignName, String state,
      Integer clicks, BigDecimal cost, Integer impressions, BigDecimal sales, Integer purchases) {
    this.profileId = profileId;
    this.portfolioId = portfolioId;
    this.date = date;
    this.campaignId = campaignId;
    this.campaignName = campaignName;
    this.state = state;
    this.clicks = clicks;
    this.cost = cost;
    this.impressions = impressions;
    this.sales = sales;
    this.purchases = purchases;
  }

  protected void incrementCounter() {
    this.counter++;
  }

  protected void resetCounter() {
    this.counter = 1;
  }

  protected int compareDate(String otherDate) {
    LocalDate thisDate = LocalDate.parse(this.date);
    LocalDate other = LocalDate.parse(otherDate);

    return thisDate.compareTo(other);
  }

  protected void checkNameAndDate(T other) {
    if(!this.campaignName.equals(other.getCampaignName()) && this.compareDate(other.getDate()) == -1) {
      this.campaignName = other.getCampaignName();
      this.date = other.getDate();
    }
  }

  protected void checkStatusAndDate(T other) {
    if(!this.state.equals(other.getState()) && this.compareDate(other.getDate()) == -1) {
      this.state = other.getState();
      this.date = other.getDate();
    }
  }

  @Override
  public void finalise() {
    this.advertisingCostOfSales = finalRound(calculateACoS(this.cost, this.sales));
    this.clickThroughRate = finalRound(calculateCTR(this.clicks, this.impressions));
    this.costPerClick = finalRound(calculateCPC(this.cost, this.clicks));
    this.conversion = finalRound(calculateConversion(this.purchases, this.clicks));
  }

}

