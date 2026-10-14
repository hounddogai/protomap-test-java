package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.GetClaimRatesRequest;
import com.acme.insurance.v1.GetClaimRatesResponse;
import com.acme.insurance.v1.GetPolicyRatesRequest;
import com.acme.insurance.v1.GetPolicyRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.acme.onboarding.v1.SearchOnboardingPlansRequest;
import com.acme.onboarding.v1.SearchOnboardingPlansResponse;
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
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a claim, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ClaimCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final ClaimCalculationRequestMapper mapper;

    private final OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public ClaimCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            ClaimCalculationRequestMapper mapper,
            OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.onboardingOnboardingPlanServiceBlocking = onboardingOnboardingPlanServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a claim.
     */
    public CalculateClaimResponse calculateClaim(CalculateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.PolicyCalculationService/QuotePolicy", outbound0);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(command0.payload());

        var outbound1 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getClaim().getAssignee().getUserId())
                        .setSubmittedBy(request.getClaim().getRequester())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input1 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateClaim", request.getClaim().getClaimId());
        return CalculateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .build())
                .build();
    }

    /**
     * Estimates a claim.
     */
    public EstimateClaimResponse estimateClaim(EstimateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = SummarizeAccessPoliciesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var command0 = new DispatchCommand<>("acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies", outbound0);
        var reply0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeAccessPolicies(command0.payload());

        var outbound1 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getClaim().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainPolicyCalculationRequest, ExplainPolicyCalculationResponse> port1 = RpcPort.adapt(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainPolicyCalculation);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setVersion(reply1.getPolicy().getHolder().getVersion())
                                .build())
                        .build())
                .build();
        Function<EstimatePolicyRequest, EstimatePolicyResponse> operation2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimatePolicy;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateClaim", request.getClaim().getClaimId());
        return EstimateClaimResponse.newBuilder()
                .setBreakdown(EstimateClaimResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getPolicy().getCurrencyCode())
                        .build())
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply2.getPolicy().getContact().getEmail())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a claim.
     */
    public QuoteClaimResponse quoteClaim(QuoteClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = SimulatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePolicy(outbound0), rpcExecutor);

        var outbound1 = RevertRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertRetentionPolicyRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .setEmail(request.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertRetentionPolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteClaim", request.getClaim().getClaimId());
        return QuoteClaimResponse.newBuilder()
                .setBreakdown(QuoteClaimResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getPolicy().getHolder().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .build();
    }

    /**
     * Compares a claim.
     */
    public CompareClaimOptionsResponse compareClaimOptions(CompareClaimOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareClaimOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = RpcSupport.unpack(Any.pack(GetPolicyRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getClaim().getSla().getPolicyId())
                .build()), GetPolicyRatesRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPolicyRates(outbound0);

        var outbound1 = BeanCopies.copy(QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setCoverageId(reply0.getPolicy().getHolder().getCoverageId())
                                .build())
                        .build())
                .build(), QuotePolicyRequest.class);
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareClaimOptions", request.getClaim().getClaimId());
        return CompareClaimOptionsResponse.newBuilder()
                .setBreakdown(CompareClaimOptionsResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply1.getPolicy().getContact().getPhone())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .build();
    }

    /**
     * Recalculates a claim.
     */
    public RecalculateClaimsResponse recalculateClaims(RecalculateClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = mapper.recalculateClaimsStage0(request);
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePostingBatch(outbound0);

        var outbound1 = JsonCodec.roundTrip(BrowseAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPostingBatch().getDueAt())
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build(), BrowseAccessPoliciesRequest.class);
        var reply1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateClaims", request.getClaim().getClaimId());
        return RecalculateClaimsResponse.newBuilder()
                .setBreakdown(RecalculateClaimsResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getPostingBatch().getTotal())
                        .build())
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .putAllCustomFields(request.getClaim().getCustomFieldsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a claim.
     */
    public SimulateClaimResponse simulateClaim(SimulateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getClaim().getRequester().getEmail())
                                .setPhone(request.getClaim().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(value)).orElseThrow();

        var outbound1 = CheckPostingBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<CheckPostingBatchEligibilityRequest, CheckPostingBatchEligibilityResponse> port1 = RpcPort.adapt(ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkPostingBatchEligibility);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateClaim", request.getClaim().getClaimId());
        return SimulateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setRequester(request.getClaim().getRequester())
                        .setSla(ClaimSla.newBuilder()
                                .setDueAt(reply1.getPostingBatch().getDueAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a claim.
     */
    public ExplainClaimCalculationResponse explainClaimCalculation(ExplainClaimCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainClaimCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var results0 = new ArrayList<SearchOnboardingPlansResponse>();
        for (var element : request.getClaim().getCommentsList()) {
            SearchOnboardingPlansRequest itemRequest = SearchOnboardingPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchOnboardingPlansRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(element.getCommentId())
                        .build())
                .build();
            results0.add(onboardingOnboardingPlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchOnboardingPlans(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SearchOnboardingPlansResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SimulatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getClaim().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<SimulatePolicyRequest, SimulatePolicyResponse>() {
            @Override
            public SimulatePolicyResponse apply(SimulatePolicyRequest input) {
                return insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePolicy(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainClaimCalculation", request.getClaim().getClaimId());
        return ExplainClaimCalculationResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(request.getClaim().getAssignee())
                        .setClaimId(request.getClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a claim.
     */
    public GetClaimRatesResponse getClaimRates(GetClaimRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetClaimRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "GetClaimRates");
        return GetClaimRatesResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .build())
                .build();
    }
}
