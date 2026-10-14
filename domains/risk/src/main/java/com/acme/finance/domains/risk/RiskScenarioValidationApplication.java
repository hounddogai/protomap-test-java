package com.acme.finance.domains.risk;

import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.SearchAudiencesRequest;
import com.acme.audiences.v1.SearchAudiencesResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.labeling.v1.LabelingTaskQueryServiceGrpc;
import com.acme.labeling.v1.ResolveLabelingTasksRequest;
import com.acme.labeling.v1.ResolveLabelingTasksResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScoreRiskScenarioRequest;
import com.acme.risk.v1.ScoreRiskScenarioResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
 * Checks risk scenarios against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskScenarioValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskScenarioValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final LabelingTaskQueryServiceGrpc.LabelingTaskQueryServiceBlockingStub labelingLabelingTaskQueryServiceBlocking;

    private final RiskScenarioValidationRequestMapper mapper;

    private final RiskRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public RiskScenarioValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            LabelingTaskQueryServiceGrpc.LabelingTaskQueryServiceBlockingStub labelingLabelingTaskQueryServiceBlocking,
            RiskScenarioValidationRequestMapper mapper,
            RiskRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.labelingLabelingTaskQueryServiceBlocking = labelingLabelingTaskQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record NormalizeRiskScenarioStage1Requested(ValidateRiskRegisterRequest request, CompletableFuture<ValidateRiskRegisterResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a risk scenario.
     */
    public ValidateRiskScenarioResponse validateRiskScenario(ValidateRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = PreviewPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPolicy(value));

        var outbound1 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getRiskScenario().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<LintRiskRegistersRequest, LintRiskRegistersResponse> port1 = RpcPort.adapt(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintRiskRegisters);
        var reply1 = port1.exchange(outbound1);

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
                                .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                .setCreatedBy(request.getRiskScenario().getAudit().getCreatedBy())
                                .setEtag(request.getRiskScenario().getAudit().getEtag())
                                .setUpdatedAt(request.getRiskScenario().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return ValidateRiskScenarioResponse.newBuilder()
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .setUserId(reply1.getRiskRegister().getManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setStatus(request.getRiskScenario().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a risk scenario.
     */
    public PreviewRiskScenarioResponse previewRiskScenario(PreviewRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = CheckRiskRegisterEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getRiskScenario().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(switch (request.getRiskScenario().getOriginCase()) {
                                                    case CUSTOMER -> request.getRiskScenario().getCustomer().getEmail();
                                                    default -> "";
                                                })
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskRegisterEligibility(outbound0);

        var outbound1 = SearchAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAudiencesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskScenario().getSla().getDueAt())
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audiences.v1.AudienceService");
        var selected1 = AudienceServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.searchAudiences(outbound1);

        List<ExplainRiskRegisterDecisionResponse> results2 = request.getRiskScenario().getCommentsList().stream()
                .map(element -> ExplainRiskRegisterDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainRiskRegisterDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(element.getCreatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(reply0.getRiskRegister().getLocation().getLocality())
                                                .setPostalCode(reply0.getRiskRegister().getLocation().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskRegisterDecision(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? ExplainRiskRegisterDecisionResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return PreviewRiskScenarioResponse.newBuilder()
                .setOutcome(PreviewRiskScenarioResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRequester(request.getRiskScenario().getRequester())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Checks a risk scenario.
     */
    public CheckRiskScenarioEligibilityResponse checkRiskScenarioEligibility(CheckRiskScenarioEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRiskScenarioEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = ExplainRiskRegisterDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getRiskScenario().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(switch (request.getRiskScenario().getOriginCase()) {
                                                    case CUSTOMER -> request.getRiskScenario().getCustomer().getEmail();
                                                    default -> "";
                                                })
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskRegisterDecision(outbound0);

        var outbound1 = NormalizeRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                                .setCreatedBy(reply0.getRiskRegister().getAudit().getCreatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getRiskRegister().getManager().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<NormalizeRiskRegisterRequest, NormalizeRiskRegisterResponse> operation1 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeRiskRegister;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRiskScenarioEligibility", request.getRiskScenario().getRiskScenarioId());
        return CheckRiskScenarioEligibilityResponse.newBuilder()
                .setOutcome(CheckRiskScenarioEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRiskRegister().getAudit().getUpdatedAt())
                                .build())
                        .setPriority(request.getRiskScenario().getPriority())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Normalizes a risk scenario.
     */
    public NormalizeRiskScenarioResponse normalizeRiskScenario(NormalizeRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = mapper.normalizeRiskScenarioStage0(request);
        var reply0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneAccessPolicy(outbound0);

        var outbound1 = ValidateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getRiskScenario().getAssignee().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ValidateRiskRegisterResponse>();
        events.publishEvent(new NormalizeRiskScenarioStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return NormalizeRiskScenarioResponse.newBuilder()
                .setOutcome(NormalizeRiskScenarioResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAccessPolicy().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setPriority(reply0.getAccessPolicy().getPriority())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Explains a risk scenario.
     */
    public ExplainRiskScenarioDecisionResponse explainRiskScenarioDecision(ExplainRiskScenarioDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRiskScenarioDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = ScoreUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getRiskScenario().getAssignee().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getCustomer().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getRiskScenario().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreUnderwriting(outbound0), rpcExecutor);

        var outbound1 = ResolveLabelingTasksRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> labelingLabelingTaskQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLabelingTasks(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(switch (request.getRiskScenario().getOriginCase()) {
                                                    case CUSTOMER -> request.getRiskScenario().getCustomer().getDisplayName();
                                                    default -> "";
                                                })
                                                .setEmail(request.getRiskScenario().getRequester().getEmail())
                                                .setUserId(reply0.getUnderwriting().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskRegisters(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRiskScenarioDecision", request.getRiskScenario().getRiskScenarioId());
        return ExplainRiskScenarioDecisionResponse.newBuilder()
                .setOutcome(ExplainRiskScenarioDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getRiskRegister().getAudit().getEtag())
                                .build())
                        .setPriority(request.getRiskScenario().getPriority())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Scores a risk scenario.
     */
    public ScoreRiskScenarioResponse scoreRiskScenario(ScoreRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = PreviewRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getRiskScenario().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskRegister(outbound0), rpcExecutor);

        var outbound1 = ValidateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getDetails().getDestination().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskRegister(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return ScoreRiskScenarioResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .build())
                        .setPriority(request.getRiskScenario().getPriority())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Lints a risk scenario.
     */
    public LintRiskScenariosResponse lintRiskScenarios(LintRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = ScoreVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(switch (request.getRiskScenario().getOriginCase()) {
                                    case CUSTOMER -> request.getRiskScenario().getCustomer().getEmail();
                                    default -> "";
                                })
                                .setUserId(request.getRiskScenario().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVenue(outbound0);

        var outbound1 = JsonCodec.roundTrip(ExplainRiskRegisterDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getVenue().getAudit().getCreatedBy())
                                                .setUpdatedAt(reply0.getVenue().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), ExplainRiskRegisterDecisionRequest.class);
        var reply1 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskRegisterDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintRiskScenarios", request.getRiskScenario().getRiskScenarioId());
        return LintRiskScenariosResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Returns a risk scenario.
     */
    public GetRiskScenarioRulesResponse getRiskScenarioRules(GetRiskScenarioRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRiskScenarioRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getRiskScenarioId(), "GetRiskScenarioRules");
        return GetRiskScenarioRulesResponse.newBuilder()
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }
}
