package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.dispatch.v1.WatchDispatchOrdersRequest;
import com.acme.dispatch.v1.WatchDispatchOrdersResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.GetPolicyRulesRequest;
import com.acme.insurance.v1.GetPolicyRulesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.performance.v1.FindReviewsRequest;
import com.acme.performance.v1.FindReviewsResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks policies against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PolicyValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyValidationApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final PolicyValidationRequestMapper mapper;

    private final ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public PolicyValidationApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking,
            ApplicationEventPublisher events,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            PolicyValidationRequestMapper mapper,
            ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.dispatchDispatchOrderServiceBlocking = dispatchDispatchOrderServiceBlocking;
        this.events = events;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.mapper = mapper;
        this.performanceReviewQueryServiceBlocking = performanceReviewQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    public record CheckPolicyEligibilityStage2Requested(RecalculatePoliciesRequest request, CompletableFuture<RecalculatePoliciesResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a policy.
     */
    public ValidatePolicyResponse validatePolicy(ValidatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = EstimateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateClaim(outbound0);

        var outbound1 = SimulateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setChannel(ClaimChannelOrigin.newBuilder()
                                .setAddress(switch (reply0.getClaim().getOriginCase()) {
                                    case CHANNEL -> reply0.getClaim().getChannel().getAddress();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateClaim(outbound1);

        var outbound2 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(reply1.getClaim().getCustomer().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ExplainClaimCalculationRequest, ExplainClaimCalculationResponse> operation2 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainClaimCalculation;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getClaim().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePolicies(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePolicy", request.getPolicy().getPolicyId());
        return ValidatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setBalance(request.getPolicy().getBalance())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getCustomer().getEmail())
                                .setPhone(reply0.getClaim().getRequester().getPhone())
                                .setPreferredChannel(reply2.getClaim().getRequester().getPreferredChannel())
                                .build())
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .setPolicyId(request.getPolicyId())
                        .setStatus(reply3.getPolicy().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a policy.
     */
    public PreviewPolicyResponse previewPolicy(PreviewPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = (BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getAudit().getCreatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setOwnerId(request.getPolicy().getExternalAccountId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound0);

        var futures1 = request.getPolicy().getLimitsList().stream()
                .map(element -> RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setPausedFor(element.getPeriod())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateClaims(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? RecalculateClaimsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = mapper.previewPolicyStage2(request, reply1);
        var reply2 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteClaim(outbound2);

        var outbound3 = EstimateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setPausedFor(reply2.getClaim().getSla().getPausedFor())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage3Adapter {
            EstimateClaimResponse fetch(EstimateClaimRequest input) {
                return insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateClaim(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPolicy", request.getPolicy().getPolicyId());
        return PreviewPolicyResponse.newBuilder()
                .setOutcome(PreviewPolicyResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setBalance(request.getPolicy().getBalance())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply3.getClaim().getAssignee().getDisplayName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a policy.
     */
    public CheckPolicyEligibilityResponse checkPolicyEligibility(CheckPolicyEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPolicyEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<RecalculateClaimsRequest, RecalculateClaimsResponse> operation0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateClaims;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = JsonCodec.roundTrip(UnsubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build(), UnsubscribeFiscalPeriodUpdatesRequest.class);
        var reply1 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeFiscalPeriodUpdates(outbound1);

        var outbound2 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculatePoliciesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply1.getFiscalPeriod().getLocation().getRegion())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setToken(request.getPolicy().getCard().getToken())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<RecalculatePoliciesResponse>();
        events.publishEvent(new CheckPolicyEligibilityStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = WatchDispatchOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply1.getFiscalPeriod().getOrganizer().getEmail())
                        .setName(reply2.getPolicy().getContact().getName())
                        .setPhone(reply2.getPolicy().getContact().getPhone())
                        .build())
                .build();
        var stream3 = dispatchDispatchOrderServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDispatchOrders(outbound3);
        var reply3 = WatchDispatchOrdersResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPolicyEligibility", request.getPolicy().getPolicyId());
        return CheckPolicyEligibilityResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getFiscalPeriod().getAudit().getCreatedBy())
                                .setUpdatedBy(reply2.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes a policy.
     */
    public NormalizePolicyResponse normalizePolicy(NormalizePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ExplainClaimCalculationRequest, ExplainClaimCalculationResponse>() {
            @Override
            public ExplainClaimCalculationResponse apply(ExplainClaimCalculationRequest input) {
                return insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ExplainAuditEventDecisionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(switch (reply0.getClaim().getOriginCase()) {
                                    case CUSTOMER -> reply0.getClaim().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAuditEventDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getClaim().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAuditEventDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizePolicy", request.getPolicy().getPolicyId());
        return NormalizePolicyResponse.newBuilder()
                .setOutcome(NormalizePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setHolder(request.getPolicy().getHolder())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a policy.
     */
    public ExplainPolicyDecisionResponse explainPolicyDecision(ExplainPolicyDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPolicyDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = FindReviewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindReviewsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getOpenedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> performanceReviewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findReviews(outbound0));

        var outbound1 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ExplainClaimCalculationRequest, ExplainClaimCalculationResponse> port1 = RpcPort.adapt(insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainClaimCalculation);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPolicyDecision", request.getPolicy().getPolicyId());
        return ExplainPolicyDecisionResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getClaim().getRequester().getName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .setStatus(request.getPolicy().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a policy.
     */
    public LintPoliciesResponse lintPolicies(LintPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPolicy().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPolicies", request.getPolicy().getPolicyId());
        return LintPoliciesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setHolder(CoverageRef.newBuilder()
                                .setCoverageId(reply0.getPolicy().getHolder().getCoverageId())
                                .build())
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Returns a policy.
     */
    public GetPolicyRulesResponse getPolicyRules(GetPolicyRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPolicyRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "GetPolicyRules");

        var outbound0 = ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(request.getContext().getDeadline())
                                .build())
                        .setSource(request.getPolicyId())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ScoreAuditEventRequest, ScoreAuditEventResponse>() {
            @Override
            public ScoreAuditEventResponse apply(ScoreAuditEventRequest input) {
                return auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetPolicyRulesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
