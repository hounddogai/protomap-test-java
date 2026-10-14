package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ArchivePostingBatchRequest;
import com.acme.ledger.v1.ArchivePostingBatchResponse;
import com.acme.ledger.v1.InspectPostingBatchRequest;
import com.acme.ledger.v1.InspectPostingBatchResponse;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LockPostingBatchRequest;
import com.acme.ledger.v1.LockPostingBatchResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PurgePostingBatchRequest;
import com.acme.ledger.v1.PurgePostingBatchResponse;
import com.acme.ledger.v1.ReassignPostingBatchRequest;
import com.acme.ledger.v1.ReassignPostingBatchResponse;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.ledger.v1.RestorePostingBatchRequest;
import com.acme.ledger.v1.RestorePostingBatchResponse;
import com.acme.ledger.v1.TransferPostingBatchRequest;
import com.acme.ledger.v1.TransferPostingBatchResponse;
import com.acme.ledger.v1.UnlockPostingBatchRequest;
import com.acme.ledger.v1.UnlockPostingBatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Operator actions on posting batches that support staff run by hand. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PostingBatchAdminApplication {
    private static final Logger log = LoggerFactory.getLogger(PostingBatchAdminApplication.class);

    private final LedgerRepository repository;

    public PostingBatchAdminApplication(
            LedgerRepository repository) {
        this.repository = repository;
    }

    /**
     * Archives a posting batch.
     */
    public ArchivePostingBatchResponse archivePostingBatch(ArchivePostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ArchivePostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "ArchivePostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ArchivePostingBatch", request.getDetails().getReasonCode());
        return ArchivePostingBatchResponse.newBuilder()
                .setOutcome(ArchivePostingBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Restores a posting batch.
     */
    public RestorePostingBatchResponse restorePostingBatch(RestorePostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RestorePostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "RestorePostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RestorePostingBatch", request.getDetails().getReasonCode());
        return RestorePostingBatchResponse.newBuilder()
                .setOutcome(RestorePostingBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Deletes a posting batch.
     */
    public PurgePostingBatchResponse purgePostingBatch(PurgePostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PurgePostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "PurgePostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PurgePostingBatch", request.getPostingBatchId());
        return PurgePostingBatchResponse.getDefaultInstance();
    }

    /**
     * Reassigns a posting batch.
     */
    public ReassignPostingBatchResponse reassignPostingBatch(ReassignPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReassignPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "ReassignPostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReassignPostingBatch", request.getDetails().getAssignee().getUserId());
        return ReassignPostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Locks a posting batch.
     */
    public LockPostingBatchResponse lockPostingBatch(LockPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LockPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "LockPostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LockPostingBatch", request.getDetails().getReasonCode());
        return LockPostingBatchResponse.newBuilder()
                .setOutcome(LockPostingBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Unlocks a posting batch.
     */
    public UnlockPostingBatchResponse unlockPostingBatch(UnlockPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnlockPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "UnlockPostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnlockPostingBatch", request.getDetails().getReasonCode());
        return UnlockPostingBatchResponse.newBuilder()
                .setOutcome(UnlockPostingBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Transfers a posting batch.
     */
    public TransferPostingBatchResponse transferPostingBatch(TransferPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TransferPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "TransferPostingBatch");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TransferPostingBatch", request.getDetails().getReasonCode());
        return TransferPostingBatchResponse.newBuilder()
                .setOutcome(TransferPostingBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Repairs a posting batch.
     */
    public RepairPostingBatchResponse repairPostingBatch(RepairPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RepairPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RepairPostingBatch", request.getPostingBatch().getPostingBatchId());
        return RepairPostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(request.getPostingBatch().getBillingAddress())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Returns a posting batch.
     */
    public InspectPostingBatchResponse inspectPostingBatch(InspectPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "InspectPostingBatch");
        return InspectPostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }
}
