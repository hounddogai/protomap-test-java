package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CompareAuditEventVersionsRequest;
import com.acme.audit.v1.CompareAuditEventVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.GetPremiumRulesRequest;
import com.acme.insurance.v1.GetPremiumRulesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.LintPremiumsRequest;
import com.acme.insurance.v1.LintPremiumsResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.ScorePremiumRequest;
import com.acme.insurance.v1.ScorePremiumResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.performance.v1.CloneReviewRequest;
import com.acme.performance.v1.CloneReviewResponse;
import com.acme.performance.v1.Review;
import com.acme.performance.v1.ReviewCardSettlement;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
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
 * Checks premiums against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PremiumValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumValidationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PremiumValidationRequestMapper mapper;

    private final ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public PremiumValidationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PremiumValidationRequestMapper mapper,
            ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.mapper = mapper;
        this.performanceReviewServiceBlocking = performanceReviewServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a premium.
     */
    public ValidatePremiumResponse validatePremium(ValidatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = CompareCoverageOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .setDetails(CompareCoverageOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPremium().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCoverageOptions(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePremium", request.getPremium().getPremiumId());
        return ValidatePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .addLines(PremiumLine.newBuilder()
                                .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a premium.
     */
    public PreviewPremiumResponse previewPremium(PreviewPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setDetails(CalculateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPremium().getBillingAddress().getLinesList())
                                .setRecipient(request.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.CoverageCalculationService/CalculateCoverage", outbound0);
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(command0.payload());

        var outbound1 = EstimateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setNotifyEmail(reply0.getCoverage().getBlock().getNotifyEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<EstimateCoverageRequest, EstimateCoverageResponse>() {
            @Override
            public EstimateCoverageResponse apply(EstimateCoverageRequest input) {
                return insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCoverage(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPremium", request.getPremium().getPremiumId());
        return PreviewPremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCoverage().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .putAllMetadata(request.getPremium().getMetadataMap())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Checks a premium.
     */
    public CheckPremiumEligibilityResponse checkPremiumEligibility(CheckPremiumEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPremiumEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreRetentionPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getPremium().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getPremium().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreRetentionPolicyRequest, ScoreRetentionPolicyResponse> port0 = RpcPort.adapt(auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreRetentionPolicy);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPremiumEligibility", request.getPremium().getPremiumId());
        return CheckPremiumEligibilityResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setNumber(request.getPremium().getNumber())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Normalizes a premium.
     */
    public NormalizePremiumResponse normalizePremium(NormalizePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = CompareAuditEventVersionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditEventVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditEventVersions(outbound0), rpcExecutor);

        var outbound1 = CloneReviewRequest.newBuilder()
                .setContext(request.getContext())
                .setReview(Review.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPremium().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPremium().getBillingAddress().getRegion())
                                .build())
                        .setCard(ReviewCardSettlement.newBuilder()
                                .setLast4(request.getPremium().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> performanceReviewServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneReview(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (reply1.getReview().getSettlementCase()) {
                                    case CARD -> reply1.getReview().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound2);

        var outbound3 = LintPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setAccountHolder(switch (reply2.getPolicy().getFundingCase()) {
                                    case BANK -> reply2.getPolicy().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPolicies(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizePremium", request.getPremium().getPremiumId());
        return NormalizePremiumResponse.newBuilder()
                .setOutcome(NormalizePremiumResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply3.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setNumber(reply1.getReview().getNumber())
                        .setPremiumId(request.getPremiumId())
                        .setTotal(request.getPremium().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores a premium.
     */
    public ScorePremiumResponse scorePremium(ScorePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(outbound0);

        var outbound1 = ShareAuditTrailExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ShareAuditTrailExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .putAllVariables(reply0.getResult().getDetailsMap())
                        .build())
                .setReason(reply0.getCoverage().getBlock().getReason())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAuditTrailExport(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePremium", request.getPremium().getPremiumId());
        return ScorePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getAuditTrail().getAudit().getEtag())
                                .build())
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .addLines(PremiumLine.newBuilder()
                                .setDescription(reply0.getCoverage().getDescription())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints a premium.
     */
    public LintPremiumsResponse lintPremiums(LintPremiumsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPremiums")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getPremium().getCounterparty().getEmail())
                        .build())
                .build();
        var stream0 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound0);
        var reply0 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPremium().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        Function<CalculateCoverageRequest, CalculateCoverageResponse> operation1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateCoverage;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(value));

        var outbound3 = mapper.lintPremiumsStage3(request, reply2);
        var reply3 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCoverage(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPremiums", request.getPremium().getPremiumId());
        return LintPremiumsResponse.newBuilder()
                .setOutcome(LintPremiumsResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply3.getCoverage().getOwner().getEmail())
                                .build())
                        .setDueAt(request.getPremium().getDueAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a premium.
     */
    public GetPremiumRulesResponse getPremiumRules(GetPremiumRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPremiumRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "GetPremiumRules");

        var outbound0 = RpcSupport.unpack(Any.pack(EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), EstimatePolicyRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePolicy(outbound0);
        return GetPremiumRulesResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
