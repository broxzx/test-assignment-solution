package com.example.testtaskagencyamazon.data;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SPCampaignsList implements Serializable {

  private Integer totalResults;

  private List<SPCampaign> campaigns;

  private String nextToken;

}
