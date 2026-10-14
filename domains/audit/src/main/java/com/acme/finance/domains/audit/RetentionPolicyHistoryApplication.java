package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.GetRetentionPolicyVersionRequest;
import com.acme.audit.v1.GetRetentionPolicyVersionResponse;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.PruneRetentionPolicyVersionsRequest;
import com.acme.audit.v1.PruneRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.finance.platform.RpcSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of a retention policy and restores earlier ones. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RetentionPolicyHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyHistoryApplication.class);

    private final AuditRepository repository;

    public RetentionPolicyHistoryApplication(
            AuditRepository repository) {
        this.repository = repository;
    }

    /**
     * Records a retention policy.
     */
    public RecordRetentionPolicyVersionResponse recordRetentionPolicyVersion(RecordRetentionPolicyVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordRetentionPolicyVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordRetentionPolicyVersion", request.getRetentionPolicy().getRetentionPolicyId());
        return RecordRetentionPolicyVersionResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setName(request.getRetentionPolicy().getName())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Lists retention policies a page at a time.
     */
    public ListRetentionPolicyVersionsResponse listRetentionPolicyVersions(ListRetentionPolicyVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRetentionPolicyVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getContext().getTraceId(), "ListRetentionPolicyVersions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRetentionPolicyVersions", request.getPage().getPageToken());
        return ListRetentionPolicyVersionsResponse.getDefaultInstance();
    }

    /**
     * Returns a retention policy.
     */
    public GetRetentionPolicyVersionResponse getRetentionPolicyVersion(GetRetentionPolicyVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRetentionPolicyVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "GetRetentionPolicyVersion");
        return GetRetentionPolicyVersionResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Reverts a retention policy.
     */
    public RevertRetentionPolicyResponse revertRetentionPolicy(RevertRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return RevertRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setVersion(request.getDetails().getVersion())
                        .build())
                .build();
    }

    /**
     * Annotates a retention policy.
     */
    public AnnotateRetentionPolicyVersionResponse annotateRetentionPolicyVersion(AnnotateRetentionPolicyVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotateRetentionPolicyVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotateRetentionPolicyVersion", request.getRetentionPolicy().getRetentionPolicyId());
        return AnnotateRetentionPolicyVersionResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setStatus(request.getRetentionPolicy().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a retention policy.
     */
    public PruneRetentionPolicyVersionsResponse pruneRetentionPolicyVersions(PruneRetentionPolicyVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneRetentionPolicyVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "PruneRetentionPolicyVersions");
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneRetentionPolicyVersions", request.getRetentionPolicyId());
        return PruneRetentionPolicyVersionsResponse.getDefaultInstance();
    }
}
