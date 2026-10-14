package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventAdminServiceGrpc;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.GetAuditEventRulesRequest;
import com.acme.audit.v1.GetAuditEventRulesResponse;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.LockAuditEventRequest;
import com.acme.audit.v1.LockAuditEventResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks audit events against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AuditEventValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventValidationApplication.class);

    private final AuditEventAdminServiceGrpc.AuditEventAdminServiceBlockingStub auditAuditEventAdminServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final AuditRepository repository;

    public AuditEventValidationApplication(
            AuditEventAdminServiceGrpc.AuditEventAdminServiceBlockingStub auditAuditEventAdminServiceBlocking,
            ApplicationEventPublisher events,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            AuditRepository repository) {
        this.auditAuditEventAdminServiceBlocking = auditAuditEventAdminServiceBlocking;
        this.events = events;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.repository = repository;
    }

    public record GetAuditEventRulesStage0Requested(SyncRetentionPoliciesRequest request, CompletableFuture<SyncRetentionPoliciesResponse> completion, io.grpc.Context context) {}

    /**
     * Validates an audit event.
     */
    public ValidateAuditEventResponse validateAuditEvent(ValidateAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAuditEvent", request.getAuditEvent().getAuditEventId());
        return ValidateAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setCorrelationId(request.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Previews an audit event.
     */
    public PreviewAuditEventResponse previewAuditEvent(PreviewAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());

        var outbound0 = LockAuditEventRequest.newBuilder()
                .setAuditEventId(request.getAuditEvent().getAuditEventId())
                .setContext(request.getContext())
                .setDetails(LockAuditEventRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> auditAuditEventAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lockAuditEvent(value));
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getKind()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAuditEvent", request.getAuditEvent().getKind());
        return PreviewAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setSequence(request.getAuditEvent().getSequence())
                        .build())
                .setOutcome(PreviewAuditEventResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Checks an audit event.
     */
    public CheckAuditEventEligibilityResponse checkAuditEventEligibility(CheckAuditEventEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckAuditEventEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckAuditEventEligibility", request.getAuditEvent().getAuditEventId());
        return CheckAuditEventEligibilityResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getAuditEvent().getAttributesMap())
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Normalizes an audit event.
     */
    public NormalizeAuditEventResponse normalizeAuditEvent(NormalizeAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAuditEvent", request.getAuditEvent().getAuditEventId());
        return NormalizeAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setCorrelationId(request.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains an audit event.
     */
    public ExplainAuditEventDecisionResponse explainAuditEventDecision(ExplainAuditEventDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAuditEventDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());

        var outbound0 = LockAuditEventRequest.newBuilder()
                .setAuditEventId(request.getAuditEvent().getAuditEventId())
                .setContext(request.getContext())
                .setDetails(LockAuditEventRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<LockAuditEventRequest, LockAuditEventResponse> port0 = RpcPort.adapt(auditAuditEventAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lockAuditEvent);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAuditEventDecision", request.getAuditEvent().getSource());
        return ExplainAuditEventDecisionResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setKind(request.getAuditEvent().getKind())
                        .build())
                .setOutcome(ExplainAuditEventDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Scores an audit event.
     */
    public ScoreAuditEventResponse scoreAuditEvent(ScoreAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());

        var outbound0 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAuditEvent().getOccurredAt())
                        .putAllLabels(request.getAuditEvent().getAttributesMap())
                        .setOwnerId(request.getAuditEvent().getSubject().getAuditTrailId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var adapter0 = new Function<BrowseLedgerAccountsRequest, BrowseLedgerAccountsResponse>() {
            @Override
            public BrowseLedgerAccountsResponse apply(BrowseLedgerAccountsRequest input) {
                return ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreAuditEvent", request.getAuditEvent().getAuditEventId());
        return ScoreAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .setKind(request.getAuditEvent().getKind())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an audit event.
     */
    public LintAuditEventsResponse lintAuditEvents(LintAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditEvent(request.getContext().getTenantId(), request.getAuditEvent());
        log.atDebug().addKeyValue("auditValue", request.getAuditEvent().getAuditEventId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAuditEvents", request.getAuditEvent().getAuditEventId());
        return LintAuditEventsResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getAuditEvent().getAttributesMap())
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Returns an audit event.
     */
    public GetAuditEventRulesResponse getAuditEventRules(GetAuditEventRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditEventRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "GetAuditEventRules");

        var outbound0 = SyncRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SyncRetentionPoliciesResponse>();
        events.publishEvent(new GetAuditEventRulesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetAuditEventRulesResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
