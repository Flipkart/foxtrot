package com.flipkart.foxtrot.core.funnel.model.response;

import com.flipkart.foxtrot.core.funnel.model.FunnelInfo;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class FunnelEventResponseV1 implements Serializable {

    // kept for older app versions
    private String eventId;

    // kept for older app versions
    private String identifierId;

    private List<FunnelInfo> funnelInfos;
}
