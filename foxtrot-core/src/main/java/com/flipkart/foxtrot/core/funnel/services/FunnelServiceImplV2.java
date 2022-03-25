package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.util.Collections;
import java.util.List;

import static com.flipkart.foxtrot.core.funnel.util.FunnelUtil.APPROVAL_REQUEST_SUBJECT;
import static com.flipkart.foxtrot.core.funnel.util.FunnelUtil.getApprovalRequestBody;

@Singleton
public class FunnelServiceImplV2 implements FunnelService {

    private final EmailClient emailClient;

    private final FunnelService funnelService;

    private final FunnelConfiguration funnelConfiguration;


    @Inject
    public FunnelServiceImplV2(@Named("FunnelServiceImplV1") final FunnelService funnelService,
                               final EmailClient emailClient,
                               final FunnelConfiguration funnelConfiguration) {
        this.emailClient = emailClient;
        this.funnelService = funnelService;
        this.funnelConfiguration = funnelConfiguration;
    }

    @Override
    public Funnel save(Funnel funnel) {
        Funnel savedFunnel = funnelService.save(funnel);
        sendForApproval(savedFunnel.getApproverEmailId(), savedFunnel.getName(), savedFunnel.getDesc());
        return savedFunnel;
    }

    @Override
    public Funnel update(String documentId,
                         Funnel funnel) {
        Funnel updatedFunnel = funnelService.update(documentId, funnel);
        sendForApproval(updatedFunnel.getApproverEmailId(), updatedFunnel.getName(), updatedFunnel.getDesc());
        return updatedFunnel;
    }

    @Override
    public Funnel approve(String documentId) {
        return funnelService.approve(documentId);
    }

    @Override
    public Funnel reject(String documentId) {
        return funnelService.reject(documentId);
    }

    @Override
    public Funnel getFunnelByFunnelId(String funnelId) {
        return funnelService.getFunnelByFunnelId(funnelId);
    }

    @Override
    public Funnel getFunnelByDocumentId(String documentId) {
        return funnelService.getFunnelByDocumentId(documentId);
    }

    @Override
    public void delete(String documentId) {
        funnelService.delete(documentId);
    }

    @Override
    public FunnelFilterResponse searchFunnel(FilterRequest filterRequest) {
        return funnelService.searchFunnel(filterRequest);
    }

    @Override
    public FunnelDropdownConfig getDropdownValues() {
        return funnelService.getDropdownValues();
    }

    @Override
    public List<Funnel> getAll(boolean deleted) {
        return funnelService.getAll(deleted);
    }

    private void sendForApproval(String mailId,
                                 String name,
                                 String description) {
        String body = getApprovalRequestBody(name, description, funnelConfiguration.getFunnelConsoleUrl());
        Email email = Email.builder()
                .subject(APPROVAL_REQUEST_SUBJECT)
                .content(body)
                .recipients(Collections.singletonList(mailId))
                .build();
        emailClient.sendEmail(email);
    }
}
