package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.BatchGetAuditEventsRequest;
import com.acme.audit.v1.BatchGetAuditEventsResponse;
import com.acme.audit.v1.CloneAuditEventRequest;
import com.acme.audit.v1.CloneAuditEventResponse;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.DeleteAuditEventRequest;
import com.acme.audit.v1.DeleteAuditEventResponse;
import com.acme.audit.v1.GetAuditEventRequest;
import com.acme.audit.v1.GetAuditEventResponse;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.audit.v1.WatchAuditEventsRequest;
import com.acme.audit.v1.WatchAuditEventsResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages audit events: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class AuditEventApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final AuditRepository repository;

    private final Executor rpcExecutor;

    public AuditEventApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            AuditRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Creates an audit event.
     */
    public CreateAuditEventResponse createAuditEvent(CreateAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());

        var futures0 = request.getAuditEvent().getMeasurementsList().stream()
                .map(element -> PreviewJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setIssuedAt(element.getObservedAt())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewJournalEntry(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? PreviewJournalEntryResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateAuditEvent", request.getAuditEvent().getAuditEventId());
        return CreateAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setSequence(request.getAuditEvent().getSequence())
                        .setSubject(request.getAuditEvent().getSubject())
                        .build())
                .build();
    }

    /**
     * Returns an audit event.
     */
    public GetAuditEventResponse getAuditEvent(GetAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "GetAuditEvent");
        return GetAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Changes an audit event.
     */
    public UpdateAuditEventResponse updateAuditEvent(UpdateAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateAuditEvent", request.getAuditEvent().getAuditEventId());
        return UpdateAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setSource(request.getAuditEvent().getSource())
                        .setSubject(request.getAuditEvent().getSubject())
                        .build())
                .build();
    }

    /**
     * Deletes an audit event.
     */
    public DeleteAuditEventResponse deleteAuditEvent(DeleteAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "DeleteAuditEvent");

        var outbound0 = ScheduleAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var reply0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditEventExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteAuditEvent", request.getAuditEventId());
        return DeleteAuditEventResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists audit events a page at a time.
     */
    public ListAuditEventsResponse listAuditEvents(ListAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "ListAuditEvents");

        var builder0 = NormalizeJournalEntryRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .setLocale(request.getContext().getLocale())
                .build());
        builder0.setJournalEntry(JournalEntry.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeJournalEntry(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditEvents", request.getPage().getPageToken());
        return ListAuditEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several audit events at once.
     */
    public BatchGetAuditEventsResponse batchGetAuditEvents(BatchGetAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "BatchGetAuditEvents");
        return BatchGetAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Lists audit events a page at a time.
     */
    public SearchAuditEventsResponse searchAuditEvents(SearchAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "SearchAuditEvents");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchAuditEvents", request.getPage().getPageToken());
        return SearchAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of audit events as they happen.
     */
    public WatchAuditEventsResponse watchAuditEvents(WatchAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "WatchAuditEvents");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchAuditEvents", request.getFilter().getQuery());
        return WatchAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Takes a stream of audit events and answers once at the end.
     */
    public BatchCreateAuditEventsResponse batchCreateAuditEvents(BatchCreateAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveAuditEvent(request.getContext().getTenantId(), request.getRecord());
        }
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateAuditEvents", request.getHeader().getSource());
        return BatchCreateAuditEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(request.getRecord().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Creates an audit event.
     */
    public CloneAuditEventResponse cloneAuditEvent(CloneAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getKind()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneAuditEvent", request.getAuditEvent().getKind());
        return CloneAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEvent().getAuditEventId())
                        .setOccurredAt(request.getAuditEvent().getOccurredAt())
                        .build())
                .build();
    }
}
