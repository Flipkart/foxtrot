package com.flipkart.foxtrot.core.funnel.persistence;

import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;

import java.util.List;

/***
 Created by nitish.goyal on 25/09/18
 ***/
public interface FunnelStore {

    void save(Funnel funnel);

    void update(Funnel funnel);

    void delete(final String documentId);

    Funnel getByDocumentId(final String documentId);

    Funnel getByFunnelId(final String funnelId);

    List<Funnel> searchSimilar(Funnel funnel);

    List<Funnel> getAll(boolean deleted);

    FunnelFilterResponse search(FilterRequest filterRequest);

    Funnel getLatestFunnel();

    FunnelDropdownConfig getFunnelDropdownValues();
}
