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
public class FunnelEventResponseV2 implements Serializable {

    private String eventType;

    private String category;

    private List<FunnelInfo> funnelInfos;
}
