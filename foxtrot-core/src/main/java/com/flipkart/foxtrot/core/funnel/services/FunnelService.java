package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;

import java.util.List;

public interface FunnelService {

    Funnel save(Funnel funnel);

    Funnel update(String documentId,
                  Funnel funnel);

    Funnel approve(String documentId);

    Funnel reject(String documentId);

    Funnel getFunnelByFunnelId(String funnelId);

    Funnel getFunnelByDocumentId(String documentId);

    void delete(String funnelId);

    FunnelFilterResponse searchFunnel(FilterRequest filterRequest);

    FunnelDropdownConfig getDropdownValues();

    List<Funnel> getAll(boolean deleted);

}
