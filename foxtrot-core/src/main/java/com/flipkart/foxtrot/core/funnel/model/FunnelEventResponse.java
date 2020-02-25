package com.flipkart.foxtrot.core.funnel.model;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class FunnelEventResponse implements Serializable {

    @Deprecated
    // kept for older app versions
    private String eventId;

    @Deprecated
    // kept for older app versions
    private String identifierId;

    private String eventType;

    private String category;

    private List<FunnelInfo> funnelInfos;
}
