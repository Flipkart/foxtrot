package com.flipkart.foxtrot.core.funnel.services;

import static com.flipkart.foxtrot.core.exception.ErrorCode.DOCUMENT_NOT_FOUND;
import static com.flipkart.foxtrot.core.exception.ErrorCode.EXECUTION_EXCEPTION;
import static com.flipkart.foxtrot.core.exception.ErrorCode.INVALID_REQUEST;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.START_ID;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.UNASSIGNED_FUNNEL_ID;
import static com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus.APPROVED;
import static com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus.WAITING_FOR_APPROVAL;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.funnel.config.FunnelDropdownConfig;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException.FunnelExceptionBuilder;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.lock.LockedExecutor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FunnelServiceImplV1 implements FunnelService {

    private static final Logger logger = LoggerFactory.getLogger(FunnelServiceImplV1.class);

    private final FunnelStore funnelStore;

    private LockedExecutor lockedExecutor;

    private static final String FUNNEL_APPROVAL_LOCK_KEY = "FUNNEL_APPROVAL";

    @Inject
    public FunnelServiceImplV1(final FunnelStore funnelStore, final LockedExecutor lockedExecutor) {
        this.funnelStore = funnelStore;
        this.lockedExecutor = lockedExecutor;
    }

    @Override
    public Funnel save(Funnel funnel) {

        checkIfSimilarFunnelExists(funnel);

        try {
            funnel.setCreatedAt(new DateTime().toDate());
            funnel.setId(UNASSIGNED_FUNNEL_ID);
            funnel.setDocumentId(UUID.randomUUID().toString());
            funnel.setFunnelStatus(WAITING_FOR_APPROVAL);
            funnel.setDeleted(false);
            funnelStore.save(funnel);
            logger.info("Created Funnel: {}", funnel);
        } catch (Exception e) {
            throw FunnelExceptionBuilder.builder(EXECUTION_EXCEPTION, "Funnel request creation failed", e)
                    .funnelName(funnel.getName())
                    .build();
        }
        return funnel;
    }

    @Override
    public Funnel approve(String documentId) {
        return lockedExecutor
                .doItInLockV6(documentId, approveAndGenerateFunnelId(), documentId1 -> {
                    throw FunnelExceptionBuilder
                            .builder(EXECUTION_EXCEPTION, "Could not acquire lock to approve funnel")
                            .build();
                }, FUNNEL_APPROVAL_LOCK_KEY);
    }

    @Override
    public Funnel reject(String documentId) {
        Funnel savedFunnel = funnelStore.get(documentId);

        if (savedFunnel == null) {
            throw FunnelExceptionBuilder.builder(DOCUMENT_NOT_FOUND, "Funnel not found")
                    .documentId(documentId)
                    .build();
        }

        savedFunnel.setFunnelStatus(FunnelStatus.REJECTED);
        savedFunnel.setDeleted(true);
        savedFunnel.setDeletedAt(new DateTime().toDate());
        funnelStore.update(savedFunnel);
        return savedFunnel;
    }

    private Function<String, Funnel> approveAndGenerateFunnelId() {
        return documentId -> {
            Funnel savedFunnel = funnelStore.get(documentId);
            validateFunnelApprovalRequest(savedFunnel);

            if (!UNASSIGNED_FUNNEL_ID.equals(savedFunnel.getId())) {
                Funnel latestFunnel = funnelStore.getLatestFunnel();
                if (latestFunnel == null) {
                    savedFunnel.setId(START_ID);
                } else {
                    savedFunnel.setId(incrementId(latestFunnel.getId()));
                }
                savedFunnel.setApprovedAt(new DateTime().toDate());
                savedFunnel.setFunnelStatus(APPROVED);
                funnelStore.update(savedFunnel);
                return savedFunnel;
            }
            return savedFunnel;
        };
    }


    private String incrementId(String id) {
        return Long.toString(Long.parseLong(id) + 1);
    }


    @Override
    public Funnel getFunnel(String funnelId) {
        return funnelStore.getByFunnelId(funnelId);
    }

    @Override
    public void delete(String funnelId) {
        Funnel savedFunnel = funnelStore.getByFunnelId(funnelId);

        if (savedFunnel == null) {
            throw FunnelExceptionBuilder.builder(DOCUMENT_NOT_FOUND, "Funnel not found")
                    .funnelId(funnelId)
                    .build();
        }

        savedFunnel.setDeleted(true);
        savedFunnel.setDeletedAt(new DateTime().toDate());
        funnelStore.update(savedFunnel);
    }

    @Override
    public FunnelFilterResponse searchFunnel(FilterRequest filterRequest) {
        return funnelStore.search(filterRequest);
    }

    @Override
    public FunnelDropdownConfig getDropdownValues() {
        return funnelStore.getFunnelDropdownValues();
    }

    @Override
    public List<Funnel> getAll(boolean deleted) {
        return funnelStore.getAll(deleted);
    }

    private void checkIfSimilarFunnelExists(Funnel funnel) {
        // Validate if Similar Funnel already exists
        List<Funnel> existingFunnels = funnelStore.searchSimilar(funnel);
        if (CollectionUtils.isNotEmpty(existingFunnels)) {
            existingFunnels.stream()
                    .filter(existingFunnel -> !existingFunnel.isDeleted() && funnel.isSimilar(existingFunnel))
                    .findAny()
                    .ifPresent(existingFunnel -> {
                        throw FunnelExceptionBuilder
                                .builder(ErrorCode.INVALID_REQUEST,
                                        String.format("Funnel already exists with document id: %s and name: %s",
                                                existingFunnel.getDocumentId(), existingFunnel.getName()))
                                .funnelId(existingFunnel.getId())
                                .funnelName(existingFunnel.getName())
                                .documentId(existingFunnel.getDocumentId())
                                .build();
                    });
        }
    }

    private void validateFunnelApprovalRequest(Funnel funnel) {
        if (funnel == null) {
            throw FunnelExceptionBuilder.builder(DOCUMENT_NOT_FOUND, "Funnel not found")
                    .build();
        }
        if (funnel.isDeleted()) {
            throw FunnelExceptionBuilder.builder(INVALID_REQUEST, "Deleted funnel can not be approved")
                    .documentId(funnel.getDocumentId())
                    .funnelName(funnel.getName())
                    .build();
        }
    }

}
