package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CancelAuditTrailExportRequest;
import com.acme.audit.v1.CancelAuditTrailExportResponse;
import com.acme.audit.v1.GetAuditTrailExportRequest;
import com.acme.audit.v1.GetAuditTrailExportResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Exports audit trails to files and other systems. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class AuditTrailExportApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailExportApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final AuditRepository repository;

    public AuditTrailExportApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            AuditRepository repository) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.repository = repository;
    }

    /**
     * Streams every matching audit trail.
     */
    public StreamAuditTrailExportResponse streamAuditTrailExport(StreamAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "StreamAuditTrailExport");

        var outbound0 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input0 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAuditTrailExport", request.getFilter().getQuery());
        return StreamAuditTrailExportResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns an audit trail.
     */
    public GetAuditTrailExportResponse getAuditTrailExport(GetAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "GetAuditTrailExport");
        return GetAuditTrailExportResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }

    /**
     * Cancels an audit trail.
     */
    public CancelAuditTrailExportResponse cancelAuditTrailExport(CancelAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "CancelAuditTrailExport");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAuditTrailExport", request.getDetails().getReasonCode());
        return CancelAuditTrailExportResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setOutcome(CancelAuditTrailExportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Lists audit trails a page at a time.
     */
    public ListAuditTrailExportsResponse listAuditTrailExports(ListAuditTrailExportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditTrailExports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "ListAuditTrailExports");

        var outbound0 = ShareAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAuditEventExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAuditEventExport(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditTrailExports", request.getPage().getPageToken());
        return ListAuditTrailExportsResponse.newBuilder()
                .addAuditTrails(AuditTrail.newBuilder()
                        .setAuditTrailId(reply0.getAuditEvent().getSubject().getAuditTrailId())
                        .setNumber(reply0.getAuditEvent().getSubject().getNumber())
                        .build())
                .build();
    }

    /**
     * Schedules an audit trail.
     */
    public ScheduleAuditTrailExportResponse scheduleAuditTrailExport(ScheduleAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "ScheduleAuditTrailExport");

        var outbound0 = JsonCodec.roundTrip(BrowseAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccessPoliciesRequest.Filter.newBuilder()
                        .setOwnerId(request.getAuditTrailId())
                        .build())
                .build(), BrowseAccessPoliciesRequest.class);
        var reply0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleAuditTrailExport", request.getDetails().getFormat());
        return ScheduleAuditTrailExportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Retrys an audit trail.
     */
    public RetryAuditTrailExportResponse retryAuditTrailExport(RetryAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryAuditTrailExport", request.getAuditTrail().getAuditTrailId());
        return RetryAuditTrailExportResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setIssuedAt(request.getAuditTrail().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Shares an audit trail.
     */
    public ShareAuditTrailExportResponse shareAuditTrailExport(ShareAuditTrailExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareAuditTrailExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "ShareAuditTrailExport");

        var outbound0 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getVariablesMap())
                        .setOwnerId(request.getAuditTrailId())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound0);
        var reply0 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareAuditTrailExport", request.getDetails().getRecipient().getEmail());
        return ShareAuditTrailExportResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }
}
