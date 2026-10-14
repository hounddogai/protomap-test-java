package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.CompareAuditEventVersionsRequest;
import com.acme.audit.v1.CompareAuditEventVersionsResponse;
import com.acme.audit.v1.GetAuditEventVersionRequest;
import com.acme.audit.v1.GetAuditEventVersionResponse;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.PruneAuditEventVersionsRequest;
import com.acme.audit.v1.PruneAuditEventVersionsResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of an audit event and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AuditEventHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventHistoryApplication.class);

    private final AuditRepository repository;

    public AuditEventHistoryApplication(
            AuditRepository repository) {
        this.repository = repository;
    }

    /**
     * Records an audit event.
     */
    public RecordAuditEventVersionResponse recordAuditEventVersion(RecordAuditEventVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordAuditEventVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordAuditEventVersion", request.getAuditEvent().getAuditEventId());
        return RecordAuditEventVersionResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getAuditEvent().getAttributesMap())
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Lists audit events a page at a time.
     */
    public ListAuditEventVersionsResponse listAuditEventVersions(ListAuditEventVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditEventVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "ListAuditEventVersions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditEventVersions", request.getPage().getPageToken());
        return ListAuditEventVersionsResponse.getDefaultInstance();
    }

    /**
     * Returns an audit event.
     */
    public GetAuditEventVersionResponse getAuditEventVersion(GetAuditEventVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditEventVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "GetAuditEventVersion");
        return GetAuditEventVersionResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Compares an audit event.
     */
    public CompareAuditEventVersionsResponse compareAuditEventVersions(CompareAuditEventVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareAuditEventVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareAuditEventVersions", request.getAuditEvent().getAuditEventId());
        return CompareAuditEventVersionsResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setCorrelationId(request.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Reverts an audit event.
     */
    public RevertAuditEventResponse revertAuditEvent(RevertAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertAuditEvent", request.getAuditEvent().getAuditEventId());
        return RevertAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setSequence(request.getAuditEvent().getSequence())
                        .build())
                .build();
    }

    /**
     * Annotates an audit event.
     */
    public AnnotateAuditEventVersionResponse annotateAuditEventVersion(AnnotateAuditEventVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotateAuditEventVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotateAuditEventVersion", request.getAuditEvent().getAuditEventId());
        return AnnotateAuditEventVersionResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(request.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Deletes an audit event.
     */
    public PruneAuditEventVersionsResponse pruneAuditEventVersions(PruneAuditEventVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneAuditEventVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "PruneAuditEventVersions");
        log.atDebug().addKeyValue("auditValue", request.getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneAuditEventVersions", request.getAuditEventId());
        return PruneAuditEventVersionsResponse.getDefaultInstance();
    }
}
