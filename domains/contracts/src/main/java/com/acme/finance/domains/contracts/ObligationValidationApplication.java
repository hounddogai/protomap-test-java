package com.acme.finance.domains.contracts;

import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.GetObligationRulesRequest;
import com.acme.contracts.v1.GetObligationRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.ObligationStatusChange;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SummarizeFeatureFlagsRequest;
import com.acme.featureflags.v1.SummarizeFeatureFlagsResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.onboarding.v1.EquipmentRef;
import com.acme.onboarding.v1.OnboardingPlan;
import com.acme.onboarding.v1.OnboardingPlanCalculationServiceGrpc;
import com.acme.onboarding.v1.SimulateOnboardingPlanRequest;
import com.acme.onboarding.v1.SimulateOnboardingPlanResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks obligations against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ObligationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ObligationValidationApplication.class);

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final ObligationValidationRequestMapper mapper;

    private final OnboardingPlanCalculationServiceGrpc.OnboardingPlanCalculationServiceBlockingStub onboardingOnboardingPlanCalculationServiceBlocking;

    private final ContractsRepository repository;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    public ObligationValidationApplication(
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            ObligationValidationRequestMapper mapper,
            OnboardingPlanCalculationServiceGrpc.OnboardingPlanCalculationServiceBlockingStub onboardingOnboardingPlanCalculationServiceBlocking,
            ContractsRepository repository,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking) {
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.mapper = mapper;
        this.onboardingOnboardingPlanCalculationServiceBlocking = onboardingOnboardingPlanCalculationServiceBlocking;
        this.repository = repository;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
    }

    /**
     * Validates an obligation.
     */
    public ValidateObligationResponse validateObligation(ValidateObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = ExplainStressTestDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainStressTestDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStressTestDecision(value)).orElseThrow();

        var outbound1 = NormalizeRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRenewalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRenewal(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateObligation", request.getObligation().getObligationId());
        return ValidateObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(request.getObligation().getAssignee())
                        .setObligationId(request.getObligationId())
                        .build())
                .setOutcome(ValidateObligationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews an obligation.
     */
    public PreviewObligationResponse previewObligation(PreviewObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = RecalculateDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setUserId(request.getObligation().getAssignee().getUserId())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getObligation().getAssignee().getEmail())
                                        .setPreferredChannel(request.getObligation().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateDepreciations(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewObligation", request.getObligation().getObligationId());
        return PreviewObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks an obligation.
     */
    public CheckObligationEligibilityResponse checkObligationEligibility(CheckObligationEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckObligationEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = mapper.checkObligationEligibilityStage0(request);
        var reply0 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeFeatureFlags(outbound0);

        var outbound1 = QuoteCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCollateral(outbound1);

        List<QuoteObligationResponse> results2 = request.getObligation().getCommentsList().stream()
                .map(element -> QuoteObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addComments(ObligationComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteObligation(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? QuoteObligationResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CalculateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getObligation().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply3 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateObligation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckObligationEligibility", request.getObligation().getObligationId());
        return CheckObligationEligibilityResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply3.getObligation().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCollateral().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setPriority(request.getObligation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes an obligation.
     */
    public NormalizeObligationResponse normalizeObligation(NormalizeObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = mapper.normalizeObligationStage0(request);
        var reply0 = onboardingOnboardingPlanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateOnboardingPlan(outbound0);

        var outbound1 = CalculateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getOnboardingPlan().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getOnboardingPlan().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateObligation(outbound1);

        var outbound2 = ValidateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getOnboardingPlan().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(StressTestCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getOnboardingPlan().getSettlementCase()) {
                                    case CARD -> reply0.getOnboardingPlan().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStressTest(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(EstimateDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setSla(DisposalSla.newBuilder()
                                .setPolicyId(request.getObligation().getSla().getPolicyId())
                                .build())
                        .build())
                .build()), EstimateDisposalRequest.class);
        var reply3 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDisposal(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeObligation", request.getObligation().getObligationId());
        return NormalizeObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply2.getStressTest().getCounterparty().getEmail())
                                .build())
                        .putAllCustomFields(request.getObligation().getCustomFieldsMap())
                        .setObligationId(request.getObligationId())
                        .setSla(ObligationSla.newBuilder()
                                .setDueAt(reply3.getDisposal().getSla().getDueAt())
                                .setPausedFor(reply3.getDisposal().getSla().getPausedFor())
                                .build())
                        .build())
                .setOutcome(NormalizeObligationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains an obligation.
     */
    public ExplainObligationDecisionResponse explainObligationDecision(ExplainObligationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainObligationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = NormalizeRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRenewalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .setPreferredChannel(request.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getObligation().getRequester().getPhone())
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRenewal(value)).orElseThrow();

        var outbound1 = RecalculateObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getObligation().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getObligation().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.contracts.v1.ObligationCalculationService/RecalculateObligations", outbound1);
        var reply1 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateObligations(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainObligationDecision", request.getObligation().getObligationId());
        return ExplainObligationDecisionResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setPriority(reply1.getObligation().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getRenewal().getOrganizer().getEmail())
                                .build())
                        .setStatus(request.getObligation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Scores an obligation.
     */
    public ScoreObligationResponse scoreObligation(ScoreObligationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreObligation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        List<RecalculateObligationsResponse> results0 = request.getObligation().getHistoryList().stream()
                .map(element -> RecalculateObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .addHistory(ObligationStatusChange.newBuilder()
                                .setChangedAt(element.getChangedAt())
                                .setChangedBy(element.getChangedBy())
                                .setFromStatus(element.getFromStatus())
                                .setToStatus(element.getToStatus())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateObligations(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RecalculateObligationsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(switch (reply0.getObligation().getOriginCase()) {
                                    case CUSTOMER -> reply0.getObligation().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .setPhone(reply0.getObligation().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAuditEvent(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreObligation", request.getObligation().getObligationId());
        return ScoreObligationResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setObligationId(request.getObligationId())
                        .setRequester(request.getObligation().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lints an obligation.
     */
    public LintObligationsResponse lintObligations(LintObligationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintObligations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveObligation(request.getContext().getTenantId(), request.getObligation());

        var outbound0 = ExplainObligationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainObligationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getObligation().getRequester().getEmail())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getObligation().getAssignee().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getObligation().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainObligationCalculation(value)).orElseThrow();

        var outbound1 = CompareObligationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(switch (request.getObligation().getOriginCase()) {
                                    case CUSTOMER -> request.getObligation().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .setSla(ObligationSla.newBuilder()
                                .setDueAt(request.getObligation().getSla().getDueAt())
                                .build())
                        .build())
                .build();
        var reply1 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareObligationOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getObligation().getObligationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintObligations", request.getObligation().getObligationId());
        return LintObligationsResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(reply1.getObligation().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .setObligationId(request.getObligationId())
                        .setSla(ObligationSla.newBuilder()
                                .setDueAt(reply0.getObligation().getSla().getDueAt())
                                .build())
                        .setSubject(request.getObligation().getSubject())
                        .build())
                .build();
    }

    /**
     * Returns an obligation.
     */
    public GetObligationRulesResponse getObligationRules(GetObligationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetObligationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Obligation", request.getObligationId(), "GetObligationRules");

        var outbound0 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventNoticesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound0);
        var reply0 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(GetObligationRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setObligationId(request.getObligationId())
                .build()), GetObligationRatesRequest.class);
        var reply1 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getObligationRates(outbound1);
        return GetObligationRulesResponse.newBuilder()
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getObligation().getCustomer().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setObligationId(request.getObligationId())
                        .build())
                .build();
    }
}
