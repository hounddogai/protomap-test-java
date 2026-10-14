package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditEventRequest;
import com.acme.audit.v1.ArchiveAuditEventResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.InspectAuditEventRequest;
import com.acme.audit.v1.InspectAuditEventResponse;
import com.acme.audit.v1.LockAuditEventRequest;
import com.acme.audit.v1.LockAuditEventResponse;
import com.acme.audit.v1.PurgeAuditEventRequest;
import com.acme.audit.v1.PurgeAuditEventResponse;
import com.acme.audit.v1.ReassignAuditEventRequest;
import com.acme.audit.v1.ReassignAuditEventResponse;
import com.acme.audit.v1.RestoreAuditEventRequest;
import com.acme.audit.v1.RestoreAuditEventResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.TransferAuditEventRequest;
import com.acme.audit.v1.TransferAuditEventResponse;
import com.acme.audit.v1.UnlockAuditEventRequest;
import com.acme.audit.v1.UnlockAuditEventResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Operator actions on audit events that support staff run by hand. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AuditEventAdminApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventAdminApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final AuditRepository repository;

    public AuditEventAdminApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            AuditRepository repository) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.repository = repository;
    }

    /**
     * Archives an audit event.
     */
    public ArchiveAuditEventResponse archiveAuditEvent(ArchiveAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ArchiveAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "ArchiveAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ArchiveAuditEvent", request.getDetails().getReasonCode());
        return ArchiveAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(ArchiveAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Restores an audit event.
     */
    public RestoreAuditEventResponse restoreAuditEvent(RestoreAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RestoreAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "RestoreAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RestoreAuditEvent", request.getDetails().getReasonCode());
        return RestoreAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(RestoreAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Deletes an audit event.
     */
    public PurgeAuditEventResponse purgeAuditEvent(PurgeAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PurgeAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "PurgeAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PurgeAuditEvent", request.getAuditEventId());
        return PurgeAuditEventResponse.getDefaultInstance();
    }

    /**
     * Reassigns an audit event.
     */
    public ReassignAuditEventResponse reassignAuditEvent(ReassignAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReassignAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "ReassignAuditEvent");

        var outbound0 = RetryAuditTrailExportRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryAuditTrailExportRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<RetryAuditTrailExportRequest, RetryAuditTrailExportResponse>() {
            @Override
            public RetryAuditTrailExportResponse apply(RetryAuditTrailExportRequest input) {
                return auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryAuditTrailExport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReassignAuditEvent", request.getDetails().getAssignee().getUserId());
        return ReassignAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ReassignAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Locks an audit event.
     */
    public LockAuditEventResponse lockAuditEvent(LockAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LockAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "LockAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LockAuditEvent", request.getDetails().getReasonCode());
        return LockAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(LockAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Unlocks an audit event.
     */
    public UnlockAuditEventResponse unlockAuditEvent(UnlockAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnlockAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "UnlockAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnlockAuditEvent", request.getDetails().getReasonCode());
        return UnlockAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(UnlockAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Transfers an audit event.
     */
    public TransferAuditEventResponse transferAuditEvent(TransferAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TransferAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "TransferAuditEvent");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TransferAuditEvent", request.getDetails().getReasonCode());
        return TransferAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(TransferAuditEventResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Returns an audit event.
     */
    public InspectAuditEventResponse inspectAuditEvent(InspectAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "InspectAuditEvent");
        return InspectAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }
}
