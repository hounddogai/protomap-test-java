package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.GetAuditTrailSyncStateRequest;
import com.acme.audit.v1.GetAuditTrailSyncStateResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.SyncAuditTrailsRequest;
import com.acme.audit.v1.SyncAuditTrailsResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Keeps audit trails in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AuditTrailSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailSyncApplication.class);

    private final ApplicationEventPublisher events;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final AuditRepository repository;

    public AuditTrailSyncApplication(
            ApplicationEventPublisher events,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            AuditRepository repository) {
        this.events = events;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.repository = repository;
    }

    public record GetAuditTrailSyncStateStage0Requested(NormalizeJournalEntryRequest request, CompletableFuture<NormalizeJournalEntryResponse> completion, io.grpc.Context context) {}

    /**
     * Syncs an audit trail.
     */
    public SyncAuditTrailsResponse syncAuditTrails(SyncAuditTrailsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncAuditTrails")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncAuditTrails", request.getAuditTrail().getAuditTrailId());
        return SyncAuditTrailsResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .build();
    }

    /**
     * Pushs an audit trail.
     */
    public PushAuditTrailResponse pushAuditTrail(PushAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushAuditTrail", request.getAuditTrail().getAuditTrailId());
        return PushAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .build();
    }

    /**
     * Pulls an audit trail.
     */
    public PullAuditTrailsResponse pullAuditTrails(PullAuditTrailsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullAuditTrails")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        List<UnsubscribeJournalEntryUpdatesResponse> results0 = request.getAuditTrail().getLinesList().stream()
                .map(element -> UnsubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .putAllVariables(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build())
                .flatMap(value -> Stream.of(ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeJournalEntryUpdates(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? UnsubscribeJournalEntryUpdatesResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullAuditTrails", request.getAuditTrail().getAuditTrailId());
        return PullAuditTrailsResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setCounterparty(request.getAuditTrail().getCounterparty())
                        .build())
                .build();
    }

    /**
     * Reconciles an audit trail.
     */
    public ReconcileAuditTrailsResponse reconcileAuditTrails(ReconcileAuditTrailsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileAuditTrails")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileAuditTrails", request.getAuditTrail().getAuditTrailId());
        return ReconcileAuditTrailsResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setStatus(request.getAuditTrail().getStatus())
                        .build())
                .build();
    }

    /**
     * Resolves an audit trail.
     */
    public ResolveAuditTrailConflictResponse resolveAuditTrailConflict(ResolveAuditTrailConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveAuditTrailConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        var outbound0 = SubscribeJournalEntryUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<SubscribeJournalEntryUpdatesRequest, SubscribeJournalEntryUpdatesResponse>() {
            @Override
            public SubscribeJournalEntryUpdatesResponse apply(SubscribeJournalEntryUpdatesRequest input) {
                return ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveAuditTrailConflict", request.getAuditTrail().getAuditTrailId());
        return ResolveAuditTrailConflictResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .setOutcome(ResolveAuditTrailConflictResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Returns an audit trail.
     */
    public GetAuditTrailSyncStateResponse getAuditTrailSyncState(GetAuditTrailSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditTrailSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "GetAuditTrailSyncState");

        var outbound0 = NormalizeJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var completion0 = new CompletableFuture<NormalizeJournalEntryResponse>();
        events.publishEvent(new GetAuditTrailSyncStateStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetAuditTrailSyncStateResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists audit trails a page at a time.
     */
    public ListAuditTrailConflictsResponse listAuditTrailConflicts(ListAuditTrailConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditTrailConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "ListAuditTrailConflicts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditTrailConflicts", request.getPage().getPageToken());
        return ListAuditTrailConflictsResponse.getDefaultInstance();
    }
}
