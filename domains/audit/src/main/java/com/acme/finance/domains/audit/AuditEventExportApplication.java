package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.CancelAuditEventExportRequest;
import com.acme.audit.v1.CancelAuditEventExportResponse;
import com.acme.audit.v1.GetAuditEventExportRequest;
import com.acme.audit.v1.GetAuditEventExportResponse;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetryAuditEventExportRequest;
import com.acme.audit.v1.RetryAuditEventExportResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Exports audit events to files and other systems. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class AuditEventExportApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventExportApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceFutureStub ledgerPostingBatchNotificationServiceFuture;

    private final AuditEventExportRequestMapper mapper;

    private final AuditRepository repository;

    public AuditEventExportApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            ChannelRegistry channels,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceFutureStub ledgerPostingBatchNotificationServiceFuture,
            AuditEventExportRequestMapper mapper,
            AuditRepository repository) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.channels = channels;
        this.ledgerPostingBatchNotificationServiceFuture = ledgerPostingBatchNotificationServiceFuture;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Streams every matching audit event.
     */
    public StreamAuditEventExportResponse streamAuditEventExport(StreamAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "StreamAuditEventExport");

        var outbound0 = ReconcileAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(request.getContext().getDeadline())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditTrailSyncService");
        var serviceName0 = "acme.audit.v1" + "." + "AuditTrailSyncService";
        var methodName0 = "Rec" + "oncileAuditTrails";
        var descriptor0 = MethodDescriptor.<ReconcileAuditTrailsRequest, ReconcileAuditTrailsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ReconcileAuditTrailsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ReconcileAuditTrailsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAuditEventExport", request.getFilter().getQuery());
        return StreamAuditEventExportResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an audit event.
     */
    public GetAuditEventExportResponse getAuditEventExport(GetAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "GetAuditEventExport");
        return GetAuditEventExportResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Cancels an audit event.
     */
    public CancelAuditEventExportResponse cancelAuditEventExport(CancelAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "CancelAuditEventExport");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAuditEventExport", request.getDetails().getReasonCode());
        return CancelAuditEventExportResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setOutcome(CancelAuditEventExportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Lists audit events a page at a time.
     */
    public ListAuditEventExportsResponse listAuditEventExports(ListAuditEventExportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditEventExports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "ListAuditEventExports");

        var outbound0 = mapper.listAuditEventExportsStage0(request);
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAuditTrails(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditEventExports", request.getPage().getPageToken());
        return ListAuditEventExportsResponse.newBuilder()
                .addAuditEvents(AuditEvent.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Schedules an audit event.
     */
    public ScheduleAuditEventExportResponse scheduleAuditEventExport(ScheduleAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "ScheduleAuditEventExport");
        log.atDebug().addKeyValue("auditValue", request.getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleAuditEventExport", request.getAuditEventId());
        return ScheduleAuditEventExportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .build();
    }

    /**
     * Retrys an audit event.
     */
    public RetryAuditEventExportResponse retryAuditEventExport(RetryAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());

        var outbound0 = SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = ledgerPostingBatchNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryAuditEventExport", request.getAuditEvent().getAuditEventId());
        return RetryAuditEventExportResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setSequence(request.getAuditEvent().getSequence())
                        .build())
                .setOutcome(RetryAuditEventExportResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Shares an audit event.
     */
    public ShareAuditEventExportResponse shareAuditEventExport(ShareAuditEventExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareAuditEventExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "ShareAuditEventExport");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareAuditEventExport", request.getDetails().getRecipient().getEmail());
        return ShareAuditEventExportResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }
}
