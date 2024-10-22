package com.example.testtaskagencyamazon.service;

import com.example.testtaskagencyamazon.data.ActiveStatus;
import com.example.testtaskagencyamazon.data.SPCampaignsList;
import jakarta.annotation.Nullable;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class CampaignService {

  public SPCampaignsList getAllSPCampaignsByProfile(String accountId, String profileId, @Nullable List<String> portfolioIds, @Nullable List<String> campaignIds, @Nullable List<ActiveStatus> status) {
    // mocked service response, no need actions
    return new SPCampaignsList();
  }

}
