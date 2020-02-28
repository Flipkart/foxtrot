package com.flipkart.foxtrot.core.funnel.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class FunnelInfo implements Serializable {

    private String funnelId;
    private FunnelData funnelData;
}
