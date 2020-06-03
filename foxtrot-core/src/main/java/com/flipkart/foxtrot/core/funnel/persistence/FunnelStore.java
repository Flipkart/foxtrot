package com.flipkart.foxtrot.core.funnel.persistence;

import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import java.util.List;

/***
 Created by nitish.goyal on 25/09/18
 ***/
public interface FunnelStore {

    void save(Funnel funnel) throws FunnelException;

    void update(Funnel funnel) throws FoxtrotException;

    void delete(final String documentId) throws FunnelException;

    Funnel getByDocumentId(final String documentId) throws FoxtrotException;

    Funnel getByFunnelId(final String funnelId) throws FoxtrotException;

    List<Funnel> searchSimilar(Funnel funnel) throws FunnelException;

    List<Funnel> getAll(boolean deleted) throws FoxtrotException;

    FunnelFilterResponse search(FilterRequest filterRequest) throws FunnelException;

    Funnel getLatestFunnel() throws FunnelException;

    FunnelDropdownConfig getFunnelDropdownValues();
}
