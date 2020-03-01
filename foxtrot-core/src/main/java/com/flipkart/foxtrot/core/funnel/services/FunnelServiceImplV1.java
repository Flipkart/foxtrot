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
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelFilterResponse;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.lock.LockedExecutor;
import com.google.common.base.Strings;
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

        assignFunnelPercentage(funnel);

        try {
            funnel.setCreatedAt(new DateTime().toDate());
            funnel.setId(UNASSIGNED_FUNNEL_ID);
            if (Strings.isNullOrEmpty(funnel.getDocumentId())) {
                funnel.setDocumentId(UUID.randomUUID().toString());
            }
            funnel.setFunnelStatus(WAITING_FOR_APPROVAL);
            funnel.setDeleted(false);
            funnelStore.save(funnel);
            logger.info("Created Funnel: {}", funnel);
        } catch (Exception e) {
            throw new FunnelException(EXECUTION_EXCEPTION, "Funnel request creation failed", e);
        }
        return funnel;
    }

    /*
        Assign funnel start and end percentage if not provided
     */
    private void assignFunnelPercentage(Funnel funnel) {
        if (funnel.getStartPercentage() == 0 && funnel.getEndPercentage() == 0) {
            int startPercentage = (int) Math.ceil(Math.random() * (100 - funnel.getPercentage()));
            funnel.setStartPercentage(startPercentage);
            funnel.setEndPercentage(startPercentage + funnel.getPercentage());
        }
    }

    @Override
    public Funnel approve(String documentId) {
        return lockedExecutor
                .doItInLockV6(documentId, approveAndGenerateFunnelId(), documentId1 -> {
                    throw new FunnelException(EXECUTION_EXCEPTION, "Could not acquire lock to approve funnel");
                }, FUNNEL_APPROVAL_LOCK_KEY);
    }

    @Override
    public Funnel reject(String documentId) {
        Funnel savedFunnel = funnelStore.get(documentId);

        validateFunnelUpdateRequest(savedFunnel);

        if (APPROVED.equals(savedFunnel.getFunnelStatus())) {
            throw new FunnelException(INVALID_REQUEST, "Can not reject already approved funnel");
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
            validateFunnelUpdateRequest(savedFunnel);

            if (UNASSIGNED_FUNNEL_ID.equals(savedFunnel.getId())) {
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
            throw new FunnelException(DOCUMENT_NOT_FOUND, "Funnel not found");
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
                        throw new FunnelException(ErrorCode.INVALID_REQUEST,
                                String.format("Funnel already exists with document id: %s and name: %s",
                                        existingFunnel.getDocumentId(), existingFunnel.getName()));
                    });
        }
    }

    private void validateFunnelUpdateRequest(Funnel funnel) {
        if (funnel == null) {
            throw new FunnelException(DOCUMENT_NOT_FOUND, "Funnel not found");
        }
        if (funnel.isDeleted()) {
            throw new FunnelException(INVALID_REQUEST, "Deleted funnel can not be updated");
        }
    }

}
