package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.ActivateControlRequest;
import com.acme.compliance.v1.ActivateControlResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExpireControlRequest;
import com.acme.compliance.v1.ExpireControlResponse;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.GetControlLifecycleRequest;
import com.acme.compliance.v1.GetControlLifecycleResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.ReinstateControlRequest;
import com.acme.compliance.v1.ReinstateControlResponse;
import com.acme.compliance.v1.RenewControlRequest;
import com.acme.compliance.v1.RenewControlResponse;
import com.acme.compliance.v1.ResolveControlsRequest;
import com.acme.compliance.v1.ResolveControlsResponse;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.SuspendControlRequest;
import com.acme.compliance.v1.SuspendControlResponse;
import com.acme.compliance.v1.TerminateControlRequest;
import com.acme.compliance.v1.TerminateControlResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
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
 * Moves a control between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ControlLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlLifecycleApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking;

    private final BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ComplianceRepository repository;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public ControlLifecycleApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking,
            BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking,
            ApplicationEventPublisher events,
            ComplianceRepository repository,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingStandingOrderWorkflowServiceBlocking = bankingStandingOrderWorkflowServiceBlocking;
        this.billingBillingAccountServiceAsync = billingBillingAccountServiceAsync;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceAttestationValidationServiceBlocking = complianceAttestationValidationServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlQueryServiceBlocking = complianceControlQueryServiceBlocking;
        this.events = events;
        this.repository = repository;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    public record ExpireControlStage0Requested(NormalizeComplianceCaseRequest request, CompletableFuture<NormalizeComplianceCaseResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a control.
     */
    public ActivateControlResponse activateControl(ActivateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "ActivateControl");

        var outbound0 = EstimateAttestationRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDetails().getEffectiveAt())
                                .build())
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<EstimateAttestationRequest, EstimateAttestationResponse>() {
            @Override
            public EstimateAttestationResponse apply(EstimateAttestationRequest input) {
                return complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAttestation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ResolveControlsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.compliance.v1.ControlQueryService");
        var selected1 = ControlQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.resolveControls(outbound1);

        var outbound2 = BatchCreateBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBillingAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateBillingAccountsResponse.getDefaultInstance());
        var input2 = billingBillingAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBillingAccounts(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateControl", request.getDetails().getReasonCode());
        return ActivateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .setControlId(request.getControlId())
                        .build())
                .setOutcome(ActivateControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Suspends a control.
     */
    public SuspendControlResponse suspendControl(SuspendControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "SuspendControl");

        var outbound0 = BrowseControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseControlsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseControls(outbound0), rpcExecutor);

        var outbound1 = CompareAttestationOptionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAttestation(Attestation.newBuilder()
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAttestationOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendControl", request.getDetails().getReasonCode());
        return SuspendControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .build())
                .setOutcome(SuspendControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Reinstates a control.
     */
    public ReinstateControlResponse reinstateControl(ReinstateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "ReinstateControl");

        var outbound0 = StartRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<StartRiskRegisterWorkflowRequest, StartRiskRegisterWorkflowResponse> port0 = RpcPort.adapt(riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::startRiskRegisterWorkflow);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ExplainAttestationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainAttestationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ExplainAttestationDecisionRequest, ExplainAttestationDecisionResponse>() {
            @Override
            public ExplainAttestationDecisionResponse apply(ExplainAttestationDecisionRequest input) {
                return complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAttestationDecision(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateControl", request.getDetails().getReasonCode());
        return ReinstateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .build())
                .setOutcome(ReinstateControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Terminates a control.
     */
    public TerminateControlResponse terminateControl(TerminateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "TerminateControl");

        var outbound0 = ShareAttestationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAttestationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.compliance.v1.AttestationReportingService/ShareAttestationReport", outbound0);
        var reply0 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAttestationReport(command0.payload());

        var outbound1 = CompareControlOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareControlOptions(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(PauseStandingOrderWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getControl().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()), PauseStandingOrderWorkflowRequest.class);
        var reply2 = bankingStandingOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseStandingOrderWorkflow(outbound2);

        var builder3 = SuggestSearchIndexesRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setFilter(SuggestSearchIndexesRequest.Filter.newBuilder()
                .setCreatedAfter(reply2.getStandingOrder().getAudit().getUpdatedAt())
                .setRegion(reply0.getAttestation().getBillingAddress().getRegion())
                .build());
        var outbound3 = builder3.build();
        var reply3 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSearchIndexes(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateControl", request.getDetails().getReasonCode());
        return TerminateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getControl().getAudit().getCreatedBy())
                                .build())
                        .setControlId(request.getControlId())
                        .build())
                .setOutcome(TerminateControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews a control.
     */
    public RenewControlResponse renewControl(RenewControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "RenewControl");

        var outbound0 = AdvanceRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceRiskScenarioWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getRiskScenario().getRequester().getEmail())
                        .setName(reply0.getRiskScenario().getRequester().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound1);
        var reply1 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewControl", request.getDetails().getReasonCode());
        return RenewControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getRiskScenario().getAssignee().getUserId())
                                .build())
                        .build())
                .setOutcome(RenewControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Expires a control.
     */
    public ExpireControlResponse expireControl(ExpireControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "ExpireControl");

        var outbound0 = NormalizeComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setCustomer(AttestationRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<NormalizeComplianceCaseResponse>();
        events.publishEvent(new ExpireControlStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ListAuditTrailVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getComplianceCase().getAudit().getCreatedAt())
                        .setOwnerId(switch (reply0.getComplianceCase().getOriginCase()) {
                            case CUSTOMER -> reply0.getComplianceCase().getCustomer().getAttestationId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailVersions(outbound1);

        var outbound2 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getComplianceCase().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(value));

        var outbound3 = SimulateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<SimulateControlRequest, SimulateControlResponse>() {
            @Override
            public SimulateControlResponse apply(SimulateControlRequest input) {
                return complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateControl(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireControl", request.getDetails().getReasonCode());
        return ExpireControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setDescription(reply3.getControl().getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .setOutcome(ExpireControlResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a control.
     */
    public GetControlLifecycleResponse getControlLifecycle(GetControlLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetControlLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "GetControlLifecycle");
        return GetControlLifecycleResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .build())
                .build();
    }

    /**
     * Schedules a control.
     */
    public ScheduleControlExpiryResponse scheduleControlExpiry(ScheduleControlExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleControlExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = SummarizeSearchIndexesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSearchIndexesRequest.Details.newBuilder()
                        .putAllFilters(request.getDetails().getParametersMap())
                        .setPeriod(request.getControl().getEffective())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.search.v1.SearchIndexQueryService");
        var selected0 = SearchIndexQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.summarizeSearchIndexes(outbound0);

        var outbound1 = BeanCopies.copy(CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getControl().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CalculateComplianceCaseRequest.class);
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateComplianceCase(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleControlExpiry", request.getControl().getControlId());
        return ScheduleControlExpiryResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .setVersion(request.getControl().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
