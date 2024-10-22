package com.example.testtaskagencyamazon.service;

import com.example.testtaskagencyamazon.data.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.example.testtaskagencyamazon.testutils.UnitTestUtil.createRandomCampaign;
import static com.example.testtaskagencyamazon.testutils.UnitTestUtil.createRandomCampaignReport;
import static com.example.testtaskagencyamazon.utils.MathUtil.calculateCPC;
import static com.example.testtaskagencyamazon.utils.MathUtil.finalRound;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CampaignStatServiceTest {

    @Mock
    CampaignService campaignService;

    @Mock
    CampaignReportService campaignReportService;

    @InjectMocks
    CampaignStatService campaignStatService;

    @Test
    void getCampaignsStatisticByType_WhenOneReport_ThenReturnCampaignStatistics() {
        String accountId = "1", profileId = "1";

        String date = "2024-01-01";

        SPCampaign campaign = createRandomCampaign();
        SPCampaignReport campaignReport = createRandomCampaignReport(campaign, profileId, date);

        List<Long> campaignIdsLong = List.of(Long.valueOf(campaign.getCampaignId()));
        List<String> campaignIdsString = List.of(campaign.getCampaignId());

        List<SPCampaignReport> campaignReports = List.of(campaignReport);
        when(
                campaignReportService.getSPDailyCampaignReports(
                        profileId, null, campaignIdsLong, null, date, date)
        ).thenReturn(campaignReports);

        when(
                campaignService.getAllSPCampaignsByProfile(
                        accountId, profileId, null, campaignIdsString, Collections.singletonList(ActiveStatus.ENABLED)
                )
        ).thenReturn(new SPCampaignsList(1, List.of(campaign), null));

        Map<Long, SPCampaignStatistic> result = campaignStatService.getSPCampaignsStatistics(
                accountId, profileId, null, campaignIdsString, date, date,
                null, null, null, null);

        assertNotNull(result);
        assertEquals(1, result.size());

        SPCampaignStatistic campaignStatisticResult = result.get(Long.parseLong(campaign.getCampaignId()));
        assertEquals(campaign.getCampaignId(), campaignStatisticResult.getCampaignId().toString());
        assertEquals(campaign.getName(), campaignStatisticResult.getCampaignName());
        assertEquals(campaignReport.getClicks(), campaignStatisticResult.getClicks());
        assertEquals(campaignReport.getImpressions(), campaignStatisticResult.getImpressions());
        assertEquals(finalRound(campaignReport.getCost()), campaignStatisticResult.getCost());
        assertEquals(finalRound(campaignReport.getCostPerClick()), campaignStatisticResult.getCostPerClick());
        assertEquals(campaignReport.getPurchases14d(), campaignStatisticResult.getPurchases());
    }

    @Test
    void getCampaignsStatisticByType_WhenManyReport_ThenReturnCampaignStatistics() {
        String accountId = "1", profileId = "1";

        String startDate = "2024-01-01";
        String endDate = "2024-01-02";

        SPCampaign campaign = createRandomCampaign();
        SPCampaignReport campaignReport1 = createRandomCampaignReport(campaign, profileId, startDate);
        SPCampaignReport campaignReport2 = createRandomCampaignReport(campaign, profileId, endDate);

        List<Long> campaignIdsLong = List.of(Long.valueOf(campaign.getCampaignId()));
        List<String> campaignIdsString = List.of(campaign.getCampaignId());

        when(
                campaignReportService.getSPDailyCampaignReports(
                        eq(profileId), any(), eq(campaignIdsLong), any(), eq(startDate), eq(endDate))
        ).thenReturn(List.of(campaignReport1, campaignReport2));

        when(
                campaignService.getAllSPCampaignsByProfile(
                        eq(accountId), eq(profileId), any(), any(), any())
        ).thenReturn(new SPCampaignsList(1, List.of(campaign), null));

        var result = campaignStatService.getSPCampaignsStatistics(
                accountId, profileId, null, campaignIdsString, startDate, endDate,
                null, null, null, null);

        assertNotNull(result);
        assertEquals(1, result.size());

        var campaignStatisticResult = result.get(Long.parseLong(campaign.getCampaignId()));
        assertEquals(campaign.getCampaignId(), campaignStatisticResult.getCampaignId().toString());
        assertEquals(campaign.getName(), campaignStatisticResult.getCampaignName());

        Integer expectedClicks = campaignReport1.getClicks() + campaignReport2.getClicks();
        System.out.println(campaignReport1.getClicks());
        System.out.println(campaignReport2.getClicks());
        assertEquals(expectedClicks, campaignStatisticResult.getClicks());

        Integer expectedImpressions = campaignReport1.getImpressions() + campaignReport2.getImpressions();
        assertEquals(expectedImpressions, campaignStatisticResult.getImpressions());

        BigDecimal expectedCost = finalRound(campaignReport1.getCost().add(campaignReport2.getCost()));
        assertEquals(expectedCost, campaignStatisticResult.getCost());

        BigDecimal expectedCostPerClick = finalRound(calculateCPC(expectedCost, expectedClicks));
        assertEquals(expectedCostPerClick, campaignStatisticResult.getCostPerClick());

        Integer expectedPurchases = campaignReport1.getPurchases14d() + campaignReport2.getPurchases14d();
        assertEquals(expectedPurchases, campaignStatisticResult.getPurchases());
    }
}