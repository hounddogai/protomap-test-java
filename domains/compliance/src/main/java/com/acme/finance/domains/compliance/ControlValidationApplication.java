package com.acme.finance.domains.compliance;

import com.acme.careplans.v1.CarePlan;
import com.acme.careplans.v1.CarePlanCalculationServiceGrpc;
import com.acme.careplans.v1.CarePlanLine;
import com.acme.careplans.v1.CarePlanLineTax;
import com.acme.careplans.v1.QuoteCarePlanRequest;
import com.acme.careplans.v1.QuoteCarePlanResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlRouteAction;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.ScoreControlRequest;
import com.acme.compliance.v1.ScoreControlResponse;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks controls against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ControlValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlValidationApplication.class);

    private final CarePlanCalculationServiceGrpc.CarePlanCalculationServiceBlockingStub careplansCarePlanCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final ControlValidationRequestMapper mapper;

    private final ComplianceRepository repository;

    public ControlValidationApplication(
            CarePlanCalculationServiceGrpc.CarePlanCalculationServiceBlockingStub careplansCarePlanCalculationServiceBlocking,
            ChannelRegistry channels,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ApplicationEventPublisher events,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            ControlValidationRequestMapper mapper,
            ComplianceRepository repository) {
        this.careplansCarePlanCalculationServiceBlocking = careplansCarePlanCalculationServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.events = events;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record ValidateControlStage0Requested(FindVenuesRequest request, CompletableFuture<FindVenuesResponse> completion, io.grpc.Context context) {}

    public record GetControlRulesStage0Requested(GetControlRatesRequest request, CompletableFuture<GetControlRatesResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a control.
     */
    public ValidateControlResponse validateControl(ValidateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = FindVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getUpdatedAt())
                        .build())
                .build();
        var completion0 = new CompletableFuture<FindVenuesResponse>();
        events.publishEvent(new ValidateControlStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CheckPremiumEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPremiumEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.PremiumValidationService");
        var selected1 = PremiumValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.checkPremiumEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateControl", request.getControl().getControlId());
        return ValidateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(request.getControl().getPriority())
                        .build())
                .setOutcome(ValidateControlResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Previews a control.
     */
    public PreviewControlResponse previewControl(PreviewControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = RecalculateComplianceCasesRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getControl().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService");
        var selected0 = ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.recalculateComplianceCases(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewControl", request.getControl().getControlId());
        return PreviewControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .setVersion(request.getControl().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a control.
     */
    public CheckControlEligibilityResponse checkControlEligibility(CheckControlEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckControlEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = mapper.checkControlEligibilityStage0(request);
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateControl(outbound0);

        var outbound1 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getControl().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<CalculateComplianceCaseRequest, CalculateComplianceCaseResponse> operation1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateComplianceCase;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = ExplainControlCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(ExplainControlCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainControlCalculation(value));

        var outbound3 = SimulateAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getControl().getAudit().getEtag())
                                .setUpdatedAt(reply1.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage3Adapter {
            SimulateAttestationResponse fetch(SimulateAttestationRequest input) {
                return complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateAttestation(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckControlEligibility", request.getControl().getControlId());
        return CheckControlEligibilityResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .setControlId(request.getControlId())
                        .setName(request.getControl().getName())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                                .build())
                        .putAllParameters(reply2.getControl().getParametersMap())
                        .setPriority(reply1.getComplianceCase().getPriority())
                        .build())
                .setOutcome(CheckControlEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a control.
     */
    public NormalizeControlResponse normalizeControl(NormalizeControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = RecalculateControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getControl().getAudit().getUpdatedBy())
                                .build())
                        .setRoute(ControlRouteAction.newBuilder()
                                .setTarget(switch (request.getControl().getActionCase()) {
                                    case ROUTE -> request.getControl().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateControls(outbound0);

        var outbound1 = mapper.normalizeControlStage1(request, reply0);
        var reply1 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateWriteOff(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeControl", request.getControl().getControlId());
        return NormalizeControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setPriority(request.getControl().getPriority())
                        .build())
                .setOutcome(NormalizeControlResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a control.
     */
    public ScoreControlResponse scoreControl(ScoreControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = GetControlRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControl().getControlId())
                .build();
        var adapter0 = new Function<GetControlRatesRequest, GetControlRatesResponse>() {
            @Override
            public GetControlRatesResponse apply(GetControlRatesRequest input) {
                return complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getControlRates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getControl().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreControl", request.getControl().getName());
        return ScoreControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setDescription(request.getControl().getDescription())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lints a control.
     */
    public LintControlsResponse lintControls(LintControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = BeanCopies.copy(QuoteCarePlanRequest.newBuilder()
                .setCarePlan(CarePlan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .build())
                        .addLines(CarePlanLine.newBuilder()
                                .setTax(CarePlanLineTax.newBuilder()
                                        .setAmount(request.getControl().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), QuoteCarePlanRequest.class);
        var reply0 = careplansCarePlanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCarePlan(outbound0);

        var outbound1 = mapper.lintControlsStage1(request);
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUser(outbound1);

        var outbound2 = PreviewComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getUser().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewComplianceCaseRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getUser().getContact().getName())
                                .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewComplianceCase(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintControls", request.getControl().getControlId());
        return LintControlsResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCarePlan().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCarePlan().getAudit().getUpdatedAt())
                                .build())
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getUser().getContact().getEmail())
                                .build())
                        .setPriority(request.getControl().getPriority())
                        .build())
                .setOutcome(LintControlsResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a control.
     */
    public GetControlRulesResponse getControlRules(GetControlRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetControlRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "GetControlRules");

        var outbound0 = GetControlRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setControlId(request.getControlId())
                .build();
        var completion0 = new CompletableFuture<GetControlRatesResponse>();
        events.publishEvent(new GetControlRulesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = EstimateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getControl().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<EstimateUnderwritingRequest, EstimateUnderwritingResponse>() {
            @Override
            public EstimateUnderwritingResponse apply(EstimateUnderwritingRequest input) {
                return insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateUnderwriting(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        return GetControlRulesResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build();
    }
}
