package com.example.testtaskagencyamazon.testutils;

import com.example.testtaskagencyamazon.data.SPCampaign;
import com.example.testtaskagencyamazon.data.SPCampaignReport;
import com.example.testtaskagencyamazon.utils.MathUtil;
import java.math.BigDecimal;
import java.util.Random;
import org.springframework.stereotype.Component;

@Component
public class UnitTestUtil {

    public static SPCampaign createRandomCampaign() {
        return SPCampaign.builder()
                .campaignId("1")
                .portfolioId("1")
                .startDate("2024-01-01")
                .endDate("2024-01-01")
                .name("Campaign")
                .state("ENABLED")
                .build();
    }

    public static SPCampaignReport createRandomCampaignReport(
        SPCampaign spCampaign, String profileId, String date
    ) {
        Random random = new Random();

        var result = new SPCampaignReport();
        result.setProfileId(Long.valueOf(profileId));
        result.setPortfolioId(spCampaign.getPortfolioId());
        result.setCampaignId(Long.valueOf(spCampaign.getCampaignId()));
        result.setCampaignName(spCampaign.getName());
        result.setCampaignStatus(spCampaign.getState());

        result.setClicks(random.nextInt(10));
        result.setCost(BigDecimal.ONE);
        result.setCostPerClick(MathUtil.calculateCPC(result.getCost(), result.getClicks()));
        result.setDate(date);
        result.setImpressions(random.nextInt(100));
        result.setPurchases1d(random.nextInt(10));
        result.setPurchases14d(random.nextInt(10));
        result.setPurchasesSameSku1d(random.nextInt(10));
        result.setPurchasesSameSku14d(random.nextInt(10));
        result.setSales1d(BigDecimal.TEN);
        result.setSales14d(BigDecimal.TEN);
        result.setSpend(result.getCost());
        return result;
    }

}
