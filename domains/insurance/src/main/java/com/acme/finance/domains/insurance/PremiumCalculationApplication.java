package com.acme.finance.domains.insurance;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.ResolveAnomaliesRequest;
import com.acme.anomalies.v1.ResolveAnomaliesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CountFeatureFlagsRequest;
import com.acme.featureflags.v1.CountFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
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
import com.acme.insurance.v1.CalculatePremiumRequest;
import com.acme.insurance.v1.CalculatePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.EstimatePremiumRequest;
import com.acme.insurance.v1.EstimatePremiumResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.GetPremiumRatesRequest;
import com.acme.insurance.v1.GetPremiumRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePremiumRequest;
import com.acme.insurance.v1.QuotePremiumResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.SimulatePremiumRequest;
import com.acme.insurance.v1.SimulatePremiumResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.recruiting.v1.CalculateJobOpeningRequest;
import com.acme.recruiting.v1.CalculateJobOpeningResponse;
import com.acme.recruiting.v1.InterviewRef;
import com.acme.recruiting.v1.JobOpening;
import com.acme.shows.v1.CalculateShowRequest;
import com.acme.shows.v1.CalculateShowResponse;
import com.acme.shows.v1.QuoteShowRequest;
import com.acme.shows.v1.QuoteShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a premium, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PremiumCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumCalculationApplication.class);

    private final AnomalyQueryServiceGrpc.AnomalyQueryServiceBlockingStub anomaliesAnomalyQueryServiceBlocking;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceFutureStub insuranceClaimCalculationServiceFuture;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final PremiumCalculationRequestMapper mapper;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking;

    public PremiumCalculationApplication(
            AnomalyQueryServiceGrpc.AnomalyQueryServiceBlockingStub anomaliesAnomalyQueryServiceBlocking,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceFutureStub insuranceClaimCalculationServiceFuture,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            PremiumCalculationRequestMapper mapper,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking) {
        this.anomaliesAnomalyQueryServiceBlocking = anomaliesAnomalyQueryServiceBlocking;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimCalculationServiceFuture = insuranceClaimCalculationServiceFuture;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.showsShowCalculationServiceBlocking = showsShowCalculationServiceBlocking;
    }

    public record CalculatePremiumStage2Requested(CalculateJobOpeningRequest request, CompletableFuture<CalculateJobOpeningResponse> completion, io.grpc.Context context) {}

    public record RecalculatePremiumsStage0Requested(ValidateClaimRequest request, CompletableFuture<ValidateClaimResponse> completion, io.grpc.Context context) {}

    public record RecalculatePremiumsStage3Requested(RecalculateClaimsRequest request, CompletableFuture<RecalculateClaimsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a premium.
     */
    public CalculatePremiumResponse calculatePremium(CalculatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = (ResolveAnomaliesRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = anomaliesAnomalyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAnomalies(outbound0);

        var outbound1 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setToken(switch (request.getPremium().getSettlementCase()) {
                                    case CARD -> request.getPremium().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound1);

        var outbound2 = CalculateJobOpeningRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateJobOpeningRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setJobOpening(JobOpening.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(InterviewRef.newBuilder()
                                .setDisplayName(reply1.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CalculateJobOpeningResponse>();
        events.publishEvent(new CalculatePremiumStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = RpcSupport.unpack(Any.pack(ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .setUpdatedAt(reply1.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(ExplainCoverageCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), ExplainCoverageCalculationRequest.class);
        var reply3 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePremium", request.getPremium().getPremiumId());
        return CalculatePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getJobOpening().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply1.getPolicy().getContact().getEmail())
                                .build())
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a premium.
     */
    public EstimatePremiumResponse estimatePremium(EstimatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = SimulateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            SimulateClaimResponse fetch(SimulateClaimRequest input) {
                return insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateClaim(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var results1 = new ArrayList<SummarizeAccessPoliciesResponse>();
        for (var element : request.getPremium().getLinesList()) {
            SummarizeAccessPoliciesRequest itemRequest = SummarizeAccessPoliciesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeAccessPoliciesRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .build();
            results1.add(authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeAccessPolicies(itemRequest));
        }
        var reply1 = results1.isEmpty() ? SummarizeAccessPoliciesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePremium", request.getPremium().getPremiumId());
        return EstimatePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getClaim().getCustomer().getEmail())
                                .build())
                        .setNotes(request.getPremium().getNotes())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Quotes a premium.
     */
    public QuotePremiumResponse quotePremium(QuotePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(value));

        var futures1 = request.getPremium().getLinesList().stream()
                .map(element -> QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? QuoteCoverageResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePremium", request.getPremium().getPremiumId());
        return QuotePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setStatus(request.getPremium().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a premium.
     */
    public ComparePremiumOptionsResponse comparePremiumOptions(ComparePremiumOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePremiumOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = (FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPremium().getAudit().getCreatedAt())
                        .setRegion(request.getPremium().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(outbound0);

        var builder1 = RevertAuditEventRequest.newBuilder();
        builder1.putAllAnnotations(request.getAnnotationsMap());
        builder1.setAuditEvent(AuditEvent.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .setSubject(AuditTrailRef.newBuilder()
                        .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                        .setNumber(request.getPremium().getNumber())
                        .build())
                .build());
        builder1.setContext(request.getContext());
        builder1.setDetails(RevertAuditEventRequest.Details.newBuilder()
                .setSnapshot(JsonCodec.encode(request.getContext()))
                .build());
        var outbound1 = builder1.build();
        var reply1 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditEvent(outbound1);

        var outbound2 = mapper.comparePremiumOptionsStage2(request);
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePolicy(outbound2);

        var outbound3 = CalculateShowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateShowRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getPremium().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setShow(Show.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getPolicy().getAudit().getEtag())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(request.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        Function<CalculateShowRequest, CalculateShowResponse> operation3 = showsShowCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateShow;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePremiumOptions", request.getPremium().getPremiumId());
        return ComparePremiumOptionsResponse.newBuilder()
                .setBreakdown(ComparePremiumOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getBreakdown().getCurrencyCode())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setTotal(request.getPremium().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates a premium.
     */
    public RecalculatePremiumsResponse recalculatePremiums(RecalculatePremiumsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculatePremiums")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ValidateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<ValidateClaimResponse>();
        events.publishEvent(new RecalculatePremiumsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setBreached(reply0.getClaim().getSla().getBreached())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = insuranceClaimCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BeanCopies.copy(CountFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountFeatureFlagsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build(), CountFeatureFlagsRequest.class);
        var reply2 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countFeatureFlags(outbound2);

        var outbound3 = RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getClaim().getRequester().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion3 = new CompletableFuture<RecalculateClaimsResponse>();
        events.publishEvent(new RecalculatePremiumsStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculatePremiums", request.getPremium().getPremiumId());
        return RecalculatePremiumsResponse.newBuilder()
                .setBreakdown(RecalculatePremiumsResponse.Breakdown.newBuilder()
                        .setTotal(reply3.getBreakdown().getTotal())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getClaim().getCustomer().getDisplayName())
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates a premium.
     */
    public SimulatePremiumResponse simulatePremium(SimulatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(CalculateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(value)).orElseThrow();

        var outbound1 = BeanCopies.copy(ListJournalEntrySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntrySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPremium().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getCoverage().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build(), ListJournalEntrySubscriptionsRequest.class);
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntrySubscriptions(outbound1);

        var outbound2 = mapper.simulatePremiumStage2(request, reply0);
        var reply2 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareClaimOptions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulatePremium", request.getPremium().getPremiumId());
        return SimulatePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCoverage().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getPremium().getBillingAddress())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply2.getClaim().getCustomer().getDisplayName())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a premium.
     */
    public ExplainPremiumCalculationResponse explainPremiumCalculation(ExplainPremiumCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPremiumCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = BeanCopies.copy(SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .setDetails(SimulateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPremium().getBillingAddress().getLinesList())
                                .setLocality(request.getPremium().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build(), SimulateCoverageRequest.class);
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(outbound0);

        var outbound1 = QuoteShowRequest.newBuilder()
                .setContext(request.getContext())
                .setShow(Show.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<QuoteShowRequest, QuoteShowResponse> port1 = RpcPort.adapt(showsShowCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteShow);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPremiumCalculation", request.getPremium().getPremiumId());
        return ExplainPremiumCalculationResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getShow().getLocation().getLocality())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setStatus(request.getPremium().getStatus())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a premium.
     */
    public GetPremiumRatesResponse getPremiumRates(GetPremiumRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPremiumRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "GetPremiumRates");

        var outbound0 = (ExplainAccessPolicyDecisionRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAccessPolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()).toBuilder().build();
        var reply0 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAccessPolicyDecision(outbound0);

        var outbound1 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAccessPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .setRoute(CoverageRouteAction.newBuilder()
                                .setWeight(reply0.getAccessPolicy().getRoute().getWeight())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.CoverageCalculationService");
        var selected1 = CoverageCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.calculateCoverage(outbound1);
        return GetPremiumRatesResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAccessPolicy().getAudit().getUpdatedBy())
                                .build())
                        .addLines(PremiumLine.newBuilder()
                                .setDescription(reply0.getAccessPolicy().getDescription())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
