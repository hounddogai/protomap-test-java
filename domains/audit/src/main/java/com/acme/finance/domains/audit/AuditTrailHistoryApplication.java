package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.PruneAuditTrailVersionsRequest;
import com.acme.audit.v1.PruneAuditTrailVersionsResponse;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of an audit trail and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AuditTrailHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailHistoryApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AuditRepository repository;

    public AuditTrailHistoryApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AuditRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.repository = repository;
    }

    /**
     * Records an audit trail.
     */
    public RecordAuditTrailVersionResponse recordAuditTrailVersion(RecordAuditTrailVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordAuditTrailVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        var outbound0 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAuditTrail().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAuditTrail().getAudit().getCreatedBy())
                                .setEtag(request.getAuditTrail().getAudit().getEtag())
                                .setUpdatedAt(request.getAuditTrail().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input0 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordAuditTrailVersion", request.getAuditTrail().getAuditTrailId());
        return RecordAuditTrailVersionResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setBillingAddress(request.getAuditTrail().getBillingAddress())
                        .build())
                .build();
    }

    /**
     * Lists audit trails a page at a time.
     */
    public ListAuditTrailVersionsResponse listAuditTrailVersions(ListAuditTrailVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditTrailVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "ListAuditTrailVersions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditTrailVersions", request.getPage().getPageToken());
        return ListAuditTrailVersionsResponse.getDefaultInstance();
    }

    /**
     * Compares an audit trail.
     */
    public CompareAuditTrailVersionsResponse compareAuditTrailVersions(CompareAuditTrailVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareAuditTrailVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareAuditTrailVersions", request.getAuditTrail().getAuditTrailId());
        return CompareAuditTrailVersionsResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .build();
    }

    /**
     * Reverts an audit trail.
     */
    public RevertAuditTrailResponse revertAuditTrail(RevertAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertAuditTrail", request.getAuditTrail().getAuditTrailId());
        return RevertAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .build();
    }

    /**
     * Annotates an audit trail.
     */
    public AnnotateAuditTrailVersionResponse annotateAuditTrailVersion(AnnotateAuditTrailVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotateAuditTrailVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotateAuditTrailVersion", request.getAuditTrail().getAuditTrailId());
        return AnnotateAuditTrailVersionResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setStatus(request.getAuditTrail().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes an audit trail.
     */
    public PruneAuditTrailVersionsResponse pruneAuditTrailVersions(PruneAuditTrailVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneAuditTrailVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "PruneAuditTrailVersions");
        log.atDebug().addKeyValue("auditValue", request.getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneAuditTrailVersions", request.getAuditTrailId());
        return PruneAuditTrailVersionsResponse.getDefaultInstance();
    }
}
