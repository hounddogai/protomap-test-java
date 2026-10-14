package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.GetRetentionPolicySyncStateRequest;
import com.acme.audit.v1.GetRetentionPolicySyncStateResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps retention policies in step with the systems of record in other regions. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class RetentionPolicySyncApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicySyncApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceFutureStub ledgerLedgerAccountNotificationServiceFuture;

    private final AuditRepository repository;

    public RetentionPolicySyncApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceFutureStub ledgerLedgerAccountNotificationServiceFuture,
            AuditRepository repository) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceFuture = ledgerLedgerAccountNotificationServiceFuture;
        this.repository = repository;
    }

    /**
     * Syncs a retention policy.
     */
    public SyncRetentionPoliciesResponse syncRetentionPolicies(SyncRetentionPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncRetentionPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());

        var outbound0 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = ledgerLedgerAccountNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncRetentionPolicies", request.getRetentionPolicy().getRetentionPolicyId());
        return SyncRetentionPoliciesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .setPriority(request.getRetentionPolicy().getPriority())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Pushs a retention policy.
     */
    public PushRetentionPolicyResponse pushRetentionPolicy(PushRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return PushRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setName(request.getRetentionPolicy().getName())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Pulls a retention policy.
     */
    public PullRetentionPoliciesResponse pullRetentionPolicies(PullRetentionPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullRetentionPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullRetentionPolicies", request.getRetentionPolicy().getRetentionPolicyId());
        return PullRetentionPoliciesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDescription(request.getRetentionPolicy().getDescription())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Reconciles a retention policy.
     */
    public ReconcileRetentionPoliciesResponse reconcileRetentionPolicies(ReconcileRetentionPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileRetentionPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());

        var outbound0 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRetentionPolicy().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .build())
                .build();
        var stream0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound0);
        var reply0 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileRetentionPolicies", request.getRetentionPolicy().getRetentionPolicyId());
        return ReconcileRetentionPoliciesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .putAllParameters(request.getRetentionPolicy().getParametersMap())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of retention policies with a peer.
     */
    public StreamRetentionPolicyChangesResponse streamRetentionPolicyChanges(StreamRetentionPolicyChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamRetentionPolicyChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getChange());
        }
        log.atDebug().addKeyValue("auditValue", request.getChange().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamRetentionPolicyChanges", request.getChange().getRetentionPolicyId());
        return StreamRetentionPolicyChangesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setPriority(request.getChange().getPriority())
                        .build())
                .setSequence(request.getAck().getSequence())
                .build();
    }

    /**
     * Resolves a retention policy.
     */
    public ResolveRetentionPolicyConflictResponse resolveRetentionPolicyConflict(ResolveRetentionPolicyConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveRetentionPolicyConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveRetentionPolicyConflict", request.getRetentionPolicy().getRetentionPolicyId());
        return ResolveRetentionPolicyConflictResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setVersion(request.getRetentionPolicy().getVersion())
                        .build())
                .build();
    }

    /**
     * Returns a retention policy.
     */
    public GetRetentionPolicySyncStateResponse getRetentionPolicySyncState(GetRetentionPolicySyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRetentionPolicySyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "GetRetentionPolicySyncState");
        return GetRetentionPolicySyncStateResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Lists retention policies a page at a time.
     */
    public ListRetentionPolicyConflictsResponse listRetentionPolicyConflicts(ListRetentionPolicyConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRetentionPolicyConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getContext().getTraceId(), "ListRetentionPolicyConflicts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRetentionPolicyConflicts", request.getPage().getPageToken());
        return ListRetentionPolicyConflictsResponse.getDefaultInstance();
    }
}
