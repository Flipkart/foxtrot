package com.flipkart.foxtrot.core.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NodeGroupActivityConfig {

    private int vacantGroupReadRepairIntervalInMins = 5;

}
