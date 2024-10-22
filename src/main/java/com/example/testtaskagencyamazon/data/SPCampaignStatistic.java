package com.example.testtaskagencyamazon.data;

import static com.example.testtaskagencyamazon.utils.MathUtil.ROUND;
import static com.example.testtaskagencyamazon.utils.MathUtil.finalRound;

import java.io.Serializable;
import java.math.BigDecimal;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString
public class SPCampaignStatistic extends CampaignStatistic<SPCampaignStatistic> implements Serializable {

  private Integer purchasesSameSku;

  public SPCampaignStatistic(SPCampaignReport report) {
    super(report.getProfileId(),
        report.getPortfolioId() == null ? null : Long.valueOf(report.getPortfolioId()),
        report.getDate(),
        report.getCampaignId(),
        report.getCampaignName(),
        report.getCampaignStatus(),
        report.getClicks(),
        report.getSpend(),
        report.getImpressions(),
        report.getSales14d() != null ? report.getSales14d() : report.getSales1d(),
        report.getPurchases14d() != null ? report.getPurchases14d() : report.getPurchases1d());

    if (report.getPurchasesSameSku14d() != null)
      this.purchasesSameSku = report.getPurchasesSameSku14d();
    else if (report.getPurchasesSameSku1d() != null)
      this.purchasesSameSku = report.getPurchasesSameSku1d();
    else
      this.purchasesSameSku = 0;
  }

  @Override
  public SPCampaignStatistic add(SPCampaignStatistic other) {
    this.incrementCounter();

    this.checkNameAndDate(other);
    this.checkStatusAndDate(other);

    this.clicks += other.getClicks();
    this.cost = this.cost.add(other.getCost()).round(ROUND);
    this.impressions += other.getImpressions();
    this.sales = this.sales.add(other.getSales()).round(ROUND);
    this.purchases += other.getPurchases();
    this.purchasesSameSku += other.getPurchasesSameSku();

    return this;
  }

  @Override
  public void finalise() {
    super.finalise();

    this.sales = finalRound(this.sales);
    this.cost = finalRound(this.cost);

    resetCounter();
  }

  public static SPCampaignStatistic createEmptyStatistic(
      Long profileId, Long portfolioId, String date, Long campaignId, String campaignName, String state
  ) {
    SPCampaignStatistic analytic = new SPCampaignStatistic();
    analytic.setProfileId(profileId);
    analytic.setPortfolioId(portfolioId);
    analytic.setDate(date);
    analytic.setCampaignId(campaignId);
    analytic.setCampaignName(campaignName);
    analytic.setState(state);
    analytic.setClicks(0);
    analytic.setCost(BigDecimal.ZERO);
    analytic.setImpressions(0);
    analytic.setSales(BigDecimal.ZERO);
    analytic.setPurchases(0);
    analytic.setPurchasesSameSku(0);
    return analytic;
  }

}
