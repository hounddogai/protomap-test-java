package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.ListForumsRequest;
import com.acme.community.v1.ListForumsResponse;
import com.acme.compliance.v1.AdvanceAttestationWorkflowRequest;
import com.acme.compliance.v1.AdvanceAttestationWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExplainSanctionDecisionRequest;
import com.acme.compliance.v1.ExplainSanctionDecisionResponse;
import com.acme.compliance.v1.InspectAttestationWorkflowRequest;
import com.acme.compliance.v1.InspectAttestationWorkflowResponse;
import com.acme.compliance.v1.ListAttestationWorkflowsRequest;
import com.acme.compliance.v1.ListAttestationWorkflowsResponse;
import com.acme.compliance.v1.PauseAttestationWorkflowRequest;
import com.acme.compliance.v1.PauseAttestationWorkflowResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.ResumeAttestationWorkflowRequest;
import com.acme.compliance.v1.ResumeAttestationWorkflowResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.RetryAttestationWorkflowRequest;
import com.acme.compliance.v1.RetryAttestationWorkflowResponse;
import com.acme.compliance.v1.RetryControlWorkflowRequest;
import com.acme.compliance.v1.RetryControlWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.compliance.v1.StartAttestationWorkflowRequest;
import com.acme.compliance.v1.StartAttestationWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.compliance.v1.SuspendAttestationRequest;
import com.acme.compliance.v1.SuspendAttestationResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.CloneBuildingRequest;
import com.acme.workplaces.v1.CloneBuildingResponse;
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
 * Runs the long-lived workflow that takes an attestation from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AttestationWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationWorkflowApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking;

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final AttestationLifecycleServiceGrpc.AttestationLifecycleServiceBlockingStub complianceAttestationLifecycleServiceBlocking;

    private final ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking;

    private final ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking;

    private final SanctionApprovalServiceGrpc.SanctionApprovalServiceBlockingStub complianceSanctionApprovalServiceBlocking;

    private final SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final AttestationWorkflowRequestMapper mapper;

    private final ComplianceRepository repository;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking;

    public AttestationWorkflowApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking,
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            AttestationLifecycleServiceGrpc.AttestationLifecycleServiceBlockingStub complianceAttestationLifecycleServiceBlocking,
            ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking,
            ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking,
            SanctionApprovalServiceGrpc.SanctionApprovalServiceBlockingStub complianceSanctionApprovalServiceBlocking,
            SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            ApplicationEventPublisher events,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            AttestationWorkflowRequestMapper mapper,
            ComplianceRepository repository,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.communityForumServiceBlocking = communityForumServiceBlocking;
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceAttestationLifecycleServiceBlocking = complianceAttestationLifecycleServiceBlocking;
        this.complianceComplianceCaseWorkflowServiceBlocking = complianceComplianceCaseWorkflowServiceBlocking;
        this.complianceControlWorkflowServiceBlocking = complianceControlWorkflowServiceBlocking;
        this.complianceSanctionApprovalServiceBlocking = complianceSanctionApprovalServiceBlocking;
        this.complianceSanctionCalculationServiceBlocking = complianceSanctionCalculationServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.events = events;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.workplacesBuildingServiceBlocking = workplacesBuildingServiceBlocking;
    }

    public record SignalAttestationWorkflowStage1Requested(CloneAccessPolicyRequest request, CompletableFuture<CloneAccessPolicyResponse> completion, io.grpc.Context context) {}

    /**
     * Starts an attestation.
     */
    public StartAttestationWorkflowResponse startAttestationWorkflow(StartAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        List<RetryControlWorkflowResponse> results0 = request.getAttestation().getLinesList().stream()
                .map(element -> RetryControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(ControlDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(complianceControlWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryControlWorkflow(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RetryControlWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResumeComplianceCaseWorkflowRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceComplianceCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeComplianceCaseWorkflow(value));

        var outbound2 = RpcSupport.unpack(Any.pack(AdvanceLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getAttestation().getBillingAddress().getPostalCode())
                                .setRecipient(request.getAttestation().getBillingAddress().getRecipient())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setAmount(reply0.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .build()), AdvanceLoanApplicationWorkflowRequest.class);
        var reply2 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceLoanApplicationWorkflow(outbound2);

        var outbound3 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply1.getComplianceCase().getRequester().getEmail())
                        .setName(request.getDetails().getNotify().getName())
                        .build())
                .build();
        var stream3 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound3);
        var reply3 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartAttestationWorkflow", request.getAttestation().getAttestationId());
        return StartAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getLoanApplication().getBillingAddress().getCountryCode())
                                .build())
                        .setStatus(request.getAttestation().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances an attestation.
     */
    public AdvanceAttestationWorkflowResponse advanceAttestationWorkflow(AdvanceAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = StreamSanctionMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream0 = complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamSanctionMetrics(outbound0);
        var reply0 = StreamSanctionMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ListForumsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListForumsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAttestation().getIssuedAt())
                        .putAllLabels(request.getAttestation().getMetadataMap())
                        .setOwnerId(request.getAttestation().getCounterparty().getComplianceCaseId())
                        .setRegion(request.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = communityForumServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listForums(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceAttestationWorkflow", request.getAttestation().getAttestationId());
        return AdvanceAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setTotal(request.getAttestation().getTotal())
                        .build())
                .setOutcome(AdvanceAttestationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels an attestation.
     */
    public CancelAttestationWorkflowResponse cancelAttestationWorkflow(CancelAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = StartControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(StartControlWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = complianceControlWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startControlWorkflow(outbound0);

        List<RecalculateAttestationsResponse> results1 = request.getAttestation().getLinesList().stream()
                .map(element -> RecalculateAttestationsRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .addLines(AttestationLine.newBuilder()
                                .setTax(AttestationLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAttestations(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? RecalculateAttestationsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAttestationWorkflow", request.getAttestation().getAttestationId());
        return CancelAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setCounterparty(request.getAttestation().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys an attestation.
     */
    public RetryAttestationWorkflowResponse retryAttestationWorkflow(RetryAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var futures0 = request.getAttestation().getLinesList().stream()
                .map(element -> ExplainSanctionDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(request.getAttestation().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                                .build())
                                        .addChildren(Sanction.newBuilder()
                                                .putAllAttributes(element.getAttributesMap())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSanctionDecision(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ExplainSanctionDecisionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RejectAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getSanction().getLocation().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectAttestation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryAttestationWorkflow", request.getAttestation().getAttestationId());
        return RetryAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setIssuedAt(request.getAttestation().getIssuedAt())
                        .build())
                .setOutcome(RetryAttestationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Pauses an attestation.
     */
    public PauseAttestationWorkflowResponse pauseAttestationWorkflow(PauseAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = RejectSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getAttestation().getBillingAddress().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> complianceSanctionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectSanction(value));

        var outbound1 = ValidateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(request.getAttestation().getBillingAddress().getRecipient())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getSanction().getManager().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ValidateSanctionRequest, ValidateSanctionResponse>() {
            @Override
            public ValidateSanctionResponse apply(ValidateSanctionRequest input) {
                return complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSanction(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        List<CloneBuildingResponse> results2 = request.getAttestation().getLinesList().stream()
                .map(element -> CloneBuildingRequest.newBuilder()
                .setBuilding(Building.newBuilder()
                        .addChildren(Building.newBuilder()
                                .addChildren(Building.newBuilder()
                                        .putAllAttributes(element.getAttributesMap())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(workplacesBuildingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneBuilding(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CloneBuildingResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseAttestationWorkflow", request.getAttestation().getAttestationId());
        return PauseAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSanction().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getSanction().getLocation().getRegion())
                                .build())
                        .setCounterparty(request.getAttestation().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Resumes an attestation.
     */
    public ResumeAttestationWorkflowResponse resumeAttestationWorkflow(ResumeAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CommentRiskScenarioApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentRiskScenarioApproval(value));

        var outbound1 = SuspendAttestationRequest.newBuilder()
                .setAttestationId(request.getAttestation().getAttestationId())
                .setContext(request.getContext())
                .setDetails(SuspendAttestationRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(reply0.getRiskScenario().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<SuspendAttestationRequest, SuspendAttestationResponse>() {
            @Override
            public SuspendAttestationResponse apply(SuspendAttestationRequest input) {
                return complianceAttestationLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suspendAttestation(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeAttestationWorkflow", request.getAttestation().getNumber());
        return ResumeAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setCounterparty(request.getAttestation().getCounterparty())
                        .build())
                .setOutcome(ResumeAttestationWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public InspectAttestationWorkflowResponse inspectAttestationWorkflow(InspectAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "InspectAttestationWorkflow");

        var outbound0 = (CommentAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentAttestationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentAttestationApproval(outbound0);
        return InspectAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAttestation().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists attestations a page at a time.
     */
    public ListAttestationWorkflowsResponse listAttestationWorkflows(ListAttestationWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAttestationWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getContext().getTraceId(), "ListAttestationWorkflows");

        var outbound0 = mapper.listAttestationWorkflowsStage0(request);
        var reply0 = complianceSanctionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateSanction(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAttestationWorkflows", request.getPage().getPageToken());
        return ListAttestationWorkflowsResponse.newBuilder()
                .addAttestations(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getSanction().getLocation().getPostalCode())
                                .setRecipient(reply0.getSanction().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals an attestation.
     */
    public SignalAttestationWorkflowResponse signalAttestationWorkflow(SignalAttestationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalAttestationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = UnsubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeRetentionPolicyUpdates(outbound0);

        var outbound1 = CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion1 = new CompletableFuture<CloneAccessPolicyResponse>();
        events.publishEvent(new SignalAttestationWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalAttestationWorkflow", request.getAttestation().getAttestationId());
        return SignalAttestationWorkflowResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getAccessPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(request.getAttestation().getBillingAddress())
                        .addLines(AttestationLine.newBuilder()
                                .setTax(AttestationLineTax.newBuilder()
                                        .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
