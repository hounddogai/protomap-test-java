package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.banking.v1.BrowseStatementsRequest;
import com.acme.banking.v1.BrowseStatementsResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.CountStatementsRequest;
import com.acme.banking.v1.CountStatementsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.FindStatementsRequest;
import com.acme.banking.v1.FindStatementsResponse;
import com.acme.banking.v1.LintMandatesRequest;
import com.acme.banking.v1.LintMandatesResponse;
import com.acme.banking.v1.LookupStatementRequest;
import com.acme.banking.v1.LookupStatementResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PeekStatementRequest;
import com.acme.banking.v1.PeekStatementResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.SummarizeStatementsRequest;
import com.acme.banking.v1.SummarizeStatementsResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.forms.v1.SuggestFormsRequest;
import com.acme.forms.v1.SuggestFormsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about statements, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StatementQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(StatementQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final BankingRepository repository;

    public StatementQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            BankingRepository repository) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.formsFormQueryServiceBlocking = formsFormQueryServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.repository = repository;
    }

    public record ExportStatementsStage0Requested(SearchAuditEventsRequest request, CompletableFuture<SearchAuditEventsResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a statement.
     */
    public LookupStatementResponse lookupStatement(LookupStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getStatementId(), "LookupStatement");

        var builder0 = SimulateClaimRequest.newBuilder();
        builder0.setClaim(Claim.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateClaim(outbound0);

        var outbound1 = CompareMandateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareMandateOptions(outbound1);
        return LookupStatementResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setNotes(reply1.getMandate().getNotes())
                        .setStatementId(request.getStatementId())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Lists statements a page at a time.
     */
    public BrowseStatementsResponse browseStatements(BrowseStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "BrowseStatements");

        var outbound0 = BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            BatchGetFeatureFlagsResponse fetch(BatchGetFeatureFlagsRequest input) {
                return featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseStatements", request.getPage().getPageToken());
        return BrowseStatementsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists statements a page at a time.
     */
    public FindStatementsResponse findStatements(FindStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "FindStatements");

        var outbound0 = UpdateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getFilter().getLabelsMap())
                        .setCorrelationId(request.getFilter().getOwnerId())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(request.getContext().getDeadline())
                                .build())
                        .setOccurredAt(request.getFilter().getCreatedAfter())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAuditEvent(value)))
                .collect(Collectors.toList()).getFirst();

        var builder1 = ValidateStandingOrderRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setStandingOrder(StandingOrder.newBuilder()
                .setOrganizer(UserRef.newBuilder()
                        .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindStatements", request.getPage().getPageToken());
        return FindStatementsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .addStatements(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getStandingOrder().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts statements by status.
     */
    public CountStatementsResponse countStatements(CountStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "CountStatements");

        var outbound0 = BeanCopies.copy(ExplainStandingOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), ExplainStandingOrderDecisionRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStandingOrderDecision(outbound0);

        var outbound1 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getStandingOrder().getLocation().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.banking.v1.MandateCalculationService");
        var selected1 = MandateCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.estimateMandate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountStatements", request.getPage().getPageToken());
        return CountStatementsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching statement.
     */
    public ExportStatementsResponse exportStatements(ExportStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "ExportStatements");

        var outbound0 = SearchAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAuditEventsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SearchAuditEventsResponse>();
        events.publishEvent(new ExportStatementsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RpcSupport.unpack(Any.pack(ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), ValidateMandateRequest.class);
        var reply1 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(outbound1);

        var outbound2 = LintMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getMandate().getBillingAddress().getLinesList())
                                .setRegion(reply1.getMandate().getBillingAddress().getRegion())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setToken(switch (reply1.getMandate().getSettlementCase()) {
                                    case CARD -> reply1.getMandate().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMandates(outbound2);

        var outbound3 = ValidateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            ValidateStandingOrderResponse fetch(ValidateStandingOrderRequest input) {
                return bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportStatements", request.getFormat());
        return ExportStatementsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getMandate().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getMandate().getBillingAddress().getPostalCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(StandingOrderRef.newBuilder()
                                .setStandingOrderId(reply3.getStandingOrder().getStandingOrderId())
                                .build())
                        .setNumber(reply1.getMandate().getNumber())
                        .build())
                .build();
    }

    /**
     * Summarizes a statement.
     */
    public SummarizeStatementsResponse summarizeStatements(SummarizeStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getStatementId(), "SummarizeStatements");

        var outbound0 = ValidateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateStandingOrderRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(value));

        var outbound1 = SuggestFormsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestFormsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getStandingOrder().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getStandingOrder().getLocation().getRegion())
                        .build())
                .build();
        RpcPort<SuggestFormsRequest, SuggestFormsResponse> port1 = RpcPort.adapt(formsFormQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestForms);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeStatements", request.getStatementId());
        return SummarizeStatementsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a statement.
     */
    public PeekStatementResponse peekStatement(PeekStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getStatementId(), "PeekStatement");
        return PeekStatementResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }
}
