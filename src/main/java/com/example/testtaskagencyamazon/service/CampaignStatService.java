package com.example.testtaskagencyamazon.service;

import com.example.testtaskagencyamazon.data.*;
import com.example.testtaskagencyamazon.utils.ObjectsUtil;
import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.example.testtaskagencyamazon.data.ActiveStatus.ENABLED;
import static com.example.testtaskagencyamazon.data.DirectionType.DESC;

@Service
@RequiredArgsConstructor
public class CampaignStatService {

    private final CampaignService campaignService;
    private final CampaignReportService campaignReportService;

    public Map<Long, SPCampaignStatistic> getSPCampaignsStatistics(
            String accountId, String profileId,
            @Nullable List<String> portfolioIds, @Nullable List<String> campaignIds,
            String startDate, String endDate,
            @Nullable Integer pageIndex, @Nullable Integer pageSize,
            @Nullable Metric sortMetric, @Nullable DirectionType sortDirection
    ) {
        List<Long> campaignIdsLong = campaignIds == null ? null : campaignIds.stream().map(Long::parseLong).toList();

        // Get all campaign reports
        List<SPCampaignReport> reports = campaignReportService
                .getSPDailyCampaignReports(profileId, portfolioIds, campaignIdsLong, null, startDate, endDate);

        // Create a map to aggregate SPCampaignStatistic objects
        Map<Long, SPCampaignStatistic> campaignAnalyticMap = new HashMap<>();
        for (SPCampaignReport report : reports) {
            Long campaignId = report.getCampaignId();

            SPCampaignStatistic currentStatistic = new SPCampaignStatistic(report);
            if (campaignAnalyticMap.containsKey(campaignId)) {
                SPCampaignStatistic spCampaignStatistic = campaignAnalyticMap.get(campaignId);
                spCampaignStatistic.add(currentStatistic);
            } else {
                campaignAnalyticMap.put(campaignId, currentStatistic);
            }
        }

        // Get all enabled SP campaigns by profile and portfolio
        List<SPCampaign> allEnabledCampaigns = campaignService
                .getAllSPCampaignsByProfile(accountId, profileId, portfolioIds, campaignIds, Collections.singletonList(ENABLED))
                .getCampaigns();

        // Add any campaigns without reports as empty statistics
        for (SPCampaign campaign : allEnabledCampaigns) {
            Long campaignId = Long.parseLong(campaign.getCampaignId());
            if (!campaignAnalyticMap.containsKey(campaignId)) {
                String portfolioId = campaign.getPortfolioId();
                Long portfolioIdLong = ObjectsUtil.parseLong(portfolioId);

                campaignAnalyticMap.put(
                        campaignId,
                        SPCampaignStatistic.createEmptyStatistic(
                                Long.parseLong(profileId),
                                portfolioIdLong,
                                endDate,
                                campaignId,
                                campaign.getName(),
                                campaign.getState()
                        )
                );
            }
        }

        // Apply sorting if the sortMetric and sortDirection are specified
        List<SPCampaignStatistic> campaignStatistics = new ArrayList<>(campaignAnalyticMap.values());
        if (sortMetric != null && sortDirection != null) {
            Comparator<SPCampaignStatistic> sortingComparator = createSPComparator(sortMetric, sortDirection);
            campaignStatistics = campaignStatistics.stream()
                    .sorted(sortingComparator)
                    .toList();
        }

        // Apply pagination if pageIndex and pageSize are specified
        if (pageIndex != null && pageSize != null && pageIndex > 0 && pageSize > 0) {
            campaignStatistics = campaignStatistics.stream()
                    .skip((long) (pageIndex - 1) * pageSize)
                    .limit(pageSize)
                    .toList();
        }

        // Convert the final list back to a map for the return value
        Map<Long, SPCampaignStatistic> finalCampaignAnalyticMap = campaignStatistics.stream()
                .collect(Collectors.toMap(SPCampaignStatistic::getCampaignId, statistic -> statistic));

        // Finalize the statistics
        finalCampaignAnalyticMap.values().forEach(SPCampaignStatistic::finalise);

        return finalCampaignAnalyticMap;
    }

    private Comparator<SPCampaignStatistic> createSPComparator(
            Metric sortMetric, DirectionType sortDirection
    ) {
        Comparator<SPCampaignStatistic> comparator = switch (sortMetric) {
            case ACOS -> Comparator.comparing(SPCampaignStatistic::getAdvertisingCostOfSales);
            case CTR -> Comparator.comparing(SPCampaignStatistic::getClickThroughRate);
            case CPC -> Comparator.comparing(SPCampaignStatistic::getCostPerClick);
            case CONVERSION -> Comparator.comparing(SPCampaignStatistic::getConversion);
            case CLICKS -> Comparator.comparing(SPCampaignStatistic::getClicks);
            case COST, SPEND -> Comparator.comparing(SPCampaignStatistic::getCost);
            case IMPRESSIONS -> Comparator.comparing(SPCampaignStatistic::getImpressions);
            case SALES -> Comparator.comparing(SPCampaignStatistic::getSales);
            case PURCHASES -> Comparator.comparing(SPCampaignStatistic::getPurchases);
            case PURCHASES_SAME_SKU -> Comparator.comparing(SPCampaignStatistic::getPurchasesSameSku);
            default -> throw new IllegalArgumentException("Invalid metric name: " + sortMetric.name());
        };

        if (DESC.equals(sortDirection)) {
            comparator = comparator.reversed();
        }

        return comparator;
    }

}
