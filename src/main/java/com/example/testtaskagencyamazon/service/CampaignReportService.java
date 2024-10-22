package com.example.testtaskagencyamazon.service;

import com.example.testtaskagencyamazon.data.ActiveStatus;
import com.example.testtaskagencyamazon.data.SPCampaignReport;
import java.util.Collections;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * @author Pavlo Mrochko
 */
@Service
public class CampaignReportService {

  public List<SPCampaignReport> getSPDailyCampaignReports(
      String profileId, List<String> portfolioIds, List<Long> campaignIds, ActiveStatus campaignStatus,
      String startDate, String endDate
  ) {
    // mocked DB response, no need actions
    return Collections.emptyList();
  }

}
