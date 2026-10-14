package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.risk.v1.CheckExposureEligibilityRequest;
import com.acme.risk.v1.CheckExposureEligibilityResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.EstimateStressTestRequest;
import com.acme.risk.v1.EstimateStressTestResponse;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.ExplainExposureDecisionRequest;
import com.acme.risk.v1.ExplainExposureDecisionResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.risk.v1.ExposureLimit;
import com.acme.risk.v1.GetExposureRulesRequest;
import com.acme.risk.v1.GetExposureRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.PreviewExposureRequest;
import com.acme.risk.v1.PreviewExposureResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks exposures against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ExposureValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExposureValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final ExposureValidationRequestMapper mapper;

    private final RiskRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    public ExposureValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            ExposureValidationRequestMapper mapper,
            RiskRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureCalculationServiceBlocking = riskExposureCalculationServiceBlocking;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
    }

    /**
     * Validates an exposure.
     */
    public ValidateExposureResponse validateExposure(ValidateExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = JsonCodec.roundTrip(CheckStandingOrderEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getExposure().getHolder().getEmail())
                                        .setPhone(request.getExposure().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build(), CheckStandingOrderEligibilityRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkStandingOrderEligibility(outbound0);

        var outbound1 = ValidateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getStandingOrder().getLocation().getLinesList())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getStandingOrder().getOrganizer().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<ValidateRiskRegisterRequest, ValidateRiskRegisterResponse> operation1 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateRiskRegister;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .setUpdatedAt(request.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateExposure", request.getExposure().getExposureId());
        return ValidateExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .setStatus(request.getExposure().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Previews an exposure.
     */
    public PreviewExposureResponse previewExposure(PreviewExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RecalculateExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getExposure().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateExposures(outbound0);

        var outbound1 = JsonCodec.roundTrip(EstimateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExposure().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .setEmail(reply0.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .build(), EstimateStressTestRequest.class);
        var reply1 = riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateStressTest(outbound1);

        var outbound2 = EstimateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<EstimateExposureRequest, EstimateExposureResponse> port2 = RpcPort.adapt(riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateExposure);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = QuoteClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getExposure().getHolder().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getExposure().getAudit().getCreatedBy())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.ClaimCalculationService");
        var serviceName3 = "acme.insurance.v1" + "." + "ClaimCalculationService";
        var methodName3 = "Quo" + "teClaim";
        var descriptor3 = MethodDescriptor.<QuoteClaimRequest, QuoteClaimResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteClaimRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteClaimResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewExposure", request.getExposure().getExposureId());
        return PreviewExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .setContact(reply2.getExposure().getContact())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(reply2.getExposure().getHolder().getEmail())
                                .build())
                        .setStatus(request.getExposure().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks an exposure.
     */
    public CheckExposureEligibilityResponse checkExposureEligibility(CheckExposureEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckExposureEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = CheckRetentionPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRetentionPolicyEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getExposure().getContact().getPhone())
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.RetentionPolicyValidationService");
        var selected0 = RetentionPolicyValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.checkRetentionPolicyEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckExposureEligibility", request.getExposure().getExposureId());
        return CheckExposureEligibilityResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .setHolder(request.getExposure().getHolder())
                        .addLimits(ExposureLimit.newBuilder()
                                .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes an exposure.
     */
    public NormalizeExposureResponse normalizeExposure(NormalizeExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(request.getExposure().getAudit().getEtag())
                                        .build())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.RiskRegisterValidationService/LintRiskRegisters", outbound0);
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskRegisters(command0.payload());

        var outbound1 = ExplainExposureCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainExposureCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.ExposureCalculationService");
        var selected1 = ExposureCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.explainExposureCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeExposure", request.getExposure().getExposureId());
        return NormalizeExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .build())
                .setOutcome(NormalizeExposureResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains an exposure.
     */
    public ExplainExposureDecisionResponse explainExposureDecision(ExplainExposureDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExposureDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = ExplainStressTestCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainStressTestCalculationRequest, ExplainStressTestCalculationResponse> port0 = RpcPort.adapt(riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainStressTestCalculation);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RecalculateExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateExposuresRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStressTest().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getStressTest().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setCard(ExposureCardFunding.newBuilder()
                                .setLast4(switch (request.getExposure().getFundingCase()) {
                                    case CARD -> request.getExposure().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateExposures(outbound1);

        var outbound2 = ScoreUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreUnderwritingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getStressTest().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .setEmail(reply1.getExposure().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.UnderwritingValidationService");
        var serviceName2 = "acme.insurance.v1" + "." + "UnderwritingValidationService";
        var methodName2 = "Sco" + "reUnderwriting";
        var descriptor2 = MethodDescriptor.<ScoreUnderwritingRequest, ScoreUnderwritingResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreUnderwritingRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreUnderwritingResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = mapper.explainExposureDecisionStage3(request, reply1);
        var reply3 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskScenario(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExposureDecision", request.getExposure().getExposureId());
        return ExplainExposureDecisionResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getUnderwriting().getAudit().getCreatedBy())
                                .setEtag(reply0.getStressTest().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getUnderwriting().getRequester().getEmail())
                                .setName(reply1.getExposure().getContact().getName())
                                .build())
                        .setCurrencyCode(request.getExposure().getCurrencyCode())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply3.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores an exposure.
     */
    public ScoreExposureResponse scoreExposure(ScoreExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = CheckRetentionPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRetentionPolicyEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CheckRetentionPolicyEligibilityResponse fetch(CheckRetentionPolicyEligibilityRequest input) {
                return auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRetentionPolicyEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ValidatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ValidatePremiumResponse fetch(ValidatePremiumRequest input) {
                return insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePremium(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreExposure", request.getExposure().getExposureId());
        return ScoreExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(request.getExposure().getHolder())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an exposure.
     */
    public LintExposuresResponse lintExposures(LintExposuresRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintExposures")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = EstimateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                .build())
                        .setBank(ExposureBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getExposure().getFundingCase()) {
                                    case BANK -> request.getExposure().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExposure(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintExposures", request.getExposure().getExposureId());
        return LintExposuresResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setBalance(request.getExposure().getBalance())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an exposure.
     */
    public GetExposureRulesResponse getExposureRules(GetExposureRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExposureRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Exposure", request.getExposureId(), "GetExposureRules");

        var outbound0 = ValidatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePolicyRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var adapter0 = new Function<ValidatePolicyRequest, ValidatePolicyResponse>() {
            @Override
            public ValidatePolicyResponse apply(ValidatePolicyRequest input) {
                return insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePolicy(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetExposureRulesResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAvailable(reply0.getPolicy().getAvailable())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
