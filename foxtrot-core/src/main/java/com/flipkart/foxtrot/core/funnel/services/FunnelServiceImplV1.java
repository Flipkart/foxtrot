package com.flipkart.foxtrot.core.funnel.services;

import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.lock.LockedExecutor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FunnelServiceImplV1 implements FunnelService {

    private static final Logger logger = LoggerFactory.getLogger(FunnelServiceImplV1.class);
    private static final String FUNNEL_APPROVAL_LOCK_KEY = "FUNNEL_APPROVAL";
    private final FunnelStore funnelStore;
    private final LockedExecutor lockedExecutor;
    private final Random random;

    @Inject
    public FunnelServiceImplV1(final FunnelStore funnelStore, final LockedExecutor lockedExecutor) {
        this.funnelStore = funnelStore;
        this.lockedExecutor = lockedExecutor;
        this.random = new Random();
    }

    /*@Override
    public Funnel save(Funnel funnel) {

        checkIfSimilarFunnelExists(funnel);

        assignFunnelPercentage(funnel);

        try {
            funnel.setCreatedAt(new DateTime().toDate());
            funnel.setId(UNASSIGNED_FUNNEL_ID);
            if (Strings.isNullOrEmpty(funnel.getDocumentId())) {
                funnel.setDocumentId(UUID.randomUUID()
                        .toString());
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

    @Override
    public Funnel update(String documentId, Funnel funnel) {
        Funnel savedFunnel = funnelStore.get(documentId);
        validateFunnelUpdateRequest(savedFunnel);

        if (isFunnelWaitingForApproval(savedFunnel)) {
            assignFunnelPercentage(funnel);

            // set parameters which are not updatable
            funnel.setDocumentId(documentId);
            funnel.setCreatedAt(savedFunnel.getCreatedAt());
            funnel.setId(savedFunnel.getId());
            funnel.setFunnelStatus(savedFunnel.getFunnelStatus());
            funnel.setDeleted(savedFunnel.isDeleted());

            funnelStore.update(funnel);
        }
        return funnel;
    }

    *//*
        Assign funnel start and end percentage if not provided
     *//*
    private void assignFunnelPercentage(Funnel funnel) {
        if (funnel.getStartPercentage() == 0 && funnel.getEndPercentage() == 0) {
            int startPercentage = (random.nextInt(100 - funnel.getPercentage()));
            funnel.setStartPercentage(startPercentage);
            funnel.setEndPercentage(startPercentage + funnel.getPercentage());
        }
    }

    @Override
    public Funnel approve(String documentId) {
        return lockedExecutor.doItInLockV6(documentId, approveAndGenerateFunnelId(), documentId1 -> {
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

            if (isFunnelWaitingForApproval(savedFunnel)) {
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

    private boolean isFunnelWaitingForApproval(Funnel savedFunnel) {
        return UNASSIGNED_FUNNEL_ID.equals(savedFunnel.getId()) && WAITING_FOR_APPROVAL.equals(
                savedFunnel.getFunnelStatus());
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
    }*/

}
