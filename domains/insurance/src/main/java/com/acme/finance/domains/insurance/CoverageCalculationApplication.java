package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.acme.localization.v1.SummarizeTranslationKeysRequest;
import com.acme.localization.v1.SummarizeTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a coverage, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CoverageCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageCalculationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public CoverageCalculationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.localizationTranslationKeyQueryServiceBlocking = localizationTranslationKeyQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record CalculateCoverageStage1Requested(ListAuditTrailExportsRequest request, CompletableFuture<ListAuditTrailExportsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a coverage.
     */
    public CalculateCoverageResponse calculateCoverage(CalculateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ExplainClaimCalculationRequest, ExplainClaimCalculationResponse> port0 = RpcPort.adapt(insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainClaimCalculation);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getClaim().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ListAuditTrailExportsResponse>();
        events.publishEvent(new CalculateCoverageStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateCoverage", request.getCoverage().getCoverageId());
        return CalculateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getClaim().getCustomer().getEmail())
                                .build())
                        .setVersion(request.getCoverage().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates a coverage.
     */
    public EstimateCoverageResponse estimateCoverage(EstimateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = BeanCopies.copy(ResolveAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveAuditEventsRequest.class);
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditEvents(outbound0);

        var outbound1 = ResolveTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.localization.v1.TranslationKeyQueryService");
        var selected1 = TranslationKeyQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.resolveTranslationKeys(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateCoverage", request.getCoverage().getCoverageId());
        return EstimateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setStatus(request.getCoverage().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Quotes a coverage.
     */
    public QuoteCoverageResponse quoteCoverage(QuoteCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = NotifyFiscalPeriodWatchersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NotifyFiscalPeriodWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .putAllVariables(request.getCoverage().getParametersMap())
                        .build())
                .setReason(switch (request.getCoverage().getActionCase()) {
                    case BLOCK -> request.getCoverage().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyFiscalPeriodWatchers(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculatePoliciesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getFiscalPeriod().getLocation().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build()), RecalculatePoliciesRequest.class);
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteCoverage", request.getCoverage().getCoverageId());
        return QuoteCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFiscalPeriod().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getPolicy().getContact().getEmail())
                                .build())
                        .putAllParameters(request.getCoverage().getParametersMap())
                        .build())
                .build();
    }

    /**
     * Compares a coverage.
     */
    public CompareCoverageOptionsResponse compareCoverageOptions(CompareCoverageOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareCoverageOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(value));
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareCoverageOptions", request.getCoverage().getCoverageId());
        return CompareCoverageOptionsResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalculates a coverage.
     */
    public RecalculateCoveragesResponse recalculateCoverages(RecalculateCoveragesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateCoverages")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = BeanCopies.copy(ComparePolicyOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ComparePolicyOptionsRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateCoverages", request.getCoverage().getCoverageId());
        return RecalculateCoveragesResponse.newBuilder()
                .setBreakdown(RecalculateCoveragesResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .build())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setVersion(request.getCoverage().getVersion())
                        .build())
                .build();
    }

    /**
     * Simulates a coverage.
     */
    public SimulateCoverageResponse simulateCoverage(SimulateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(request.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(value)).orElseThrow();

        var outbound1 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setCoverageId(reply0.getPolicy().getHolder().getCoverageId())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateCoverage", request.getCoverage().getCoverageId());
        return SimulateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setName(request.getCoverage().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a coverage.
     */
    public ExplainCoverageCalculationResponse explainCoverageCalculation(ExplainCoverageCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCoverageCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = RevertAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditEventRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditEvent(outbound0), rpcExecutor);

        var outbound1 = RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateClaims(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(reply1.getClaim().getRequester().getName())
                                .setPhone(reply1.getClaim().getRequester().getPhone())
                                .setPreferredChannel(reply1.getClaim().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(outbound2);

        var outbound3 = SummarizeTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(switch (request.getCoverage().getActionCase()) {
                    case BLOCK -> request.getCoverage().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply3 = localizationTranslationKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeTranslationKeys(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCoverageCalculation", request.getCoverage().getCoverageId());
        return ExplainCoverageCalculationResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setOutcome(ExplainCoverageCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
