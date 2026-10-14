package com.acme.finance.domains.compliance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.ListCollectionCaseWorkflowsRequest;
import com.acme.collections.v1.ListCollectionCaseWorkflowsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.ApproveComplianceCaseRequest;
import com.acme.compliance.v1.ApproveComplianceCaseResponse;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CommentComplianceCaseApprovalRequest;
import com.acme.compliance.v1.CommentComplianceCaseApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalRequest;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalResponse;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusRequest;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.ListPendingComplianceCasesRequest;
import com.acme.compliance.v1.ListPendingComplianceCasesResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.RejectComplianceCaseRequest;
import com.acme.compliance.v1.RejectComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ScoreSanctionRequest;
import com.acme.compliance.v1.ScoreSanctionResponse;
import com.acme.compliance.v1.SubmitComplianceCaseRequest;
import com.acme.compliance.v1.SubmitComplianceCaseResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.itsm.v1.CloneIncidentRequest;
import com.acme.itsm.v1.CloneIncidentResponse;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentComment;
import com.acme.itsm.v1.IncidentServiceGrpc;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.notifications.v1.ExplainNotificationDecisionRequest;
import com.acme.notifications.v1.ExplainNotificationDecisionResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationStatusChange;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.secrets.v1.FindSecretsRequest;
import com.acme.secrets.v1.FindSecretsResponse;
import com.acme.secrets.v1.SecretQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes compliance cases through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ComplianceCaseApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseApprovalApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final IncidentServiceGrpc.IncidentServiceBlockingStub itsmIncidentServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture;

    private final PayRunServiceGrpc.PayRunServiceStub payrollPayRunServiceAsync;

    private final ComplianceRepository repository;

    private final ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SecretQueryServiceGrpc.SecretQueryServiceBlockingStub secretsSecretQueryServiceBlocking;

    public ComplianceCaseApprovalApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            ApplicationEventPublisher events,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            IncidentServiceGrpc.IncidentServiceBlockingStub itsmIncidentServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture,
            PayRunServiceGrpc.PayRunServiceStub payrollPayRunServiceAsync,
            ComplianceRepository repository,
            ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SecretQueryServiceGrpc.SecretQueryServiceBlockingStub secretsSecretQueryServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.events = events;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.itsmIncidentServiceBlocking = itsmIncidentServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.notificationsNotificationValidationServiceFuture = notificationsNotificationValidationServiceFuture;
        this.payrollPayRunServiceAsync = payrollPayRunServiceAsync;
        this.repository = repository;
        this.riskExposureApprovalServiceBlocking = riskExposureApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.secretsSecretQueryServiceBlocking = secretsSecretQueryServiceBlocking;
    }

    public record RejectComplianceCaseStage0Requested(EstimateControlRequest request, CompletableFuture<EstimateControlResponse> completion, io.grpc.Context context) {}

    public record GetComplianceCaseApprovalStatusStage0Requested(CalculateComplianceCaseRequest request, CompletableFuture<CalculateComplianceCaseResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a compliance case.
     */
    public SubmitComplianceCaseResponse submitComplianceCase(SubmitComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = JsonCodec.roundTrip(ScoreSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build(), ScoreSanctionRequest.class);
        var reply0 = complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSanction(outbound0);

        List<PullUnderwritingsResponse> results1 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> PullUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setBody(element.getBody())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullUnderwritings(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? PullUnderwritingsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreatePayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePayRunsRequest.StreamHeader.newBuilder()
                        .setSource(request.getComplianceCase().getAssignee().getUserId())
                        .setSubmittedBy(request.getComplianceCase().getRequester())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreatePayRunsResponse.getDefaultInstance());
        var input2 = payrollPayRunServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePayRuns(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                .setEtag(request.getComplianceCase().getAudit().getEtag())
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return SubmitComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setOutcome(SubmitComplianceCaseResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves a compliance case.
     */
    public ApproveComplianceCaseResponse approveComplianceCase(ApproveComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = BrowseJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getAudit().getCreatedAt())
                        .putAllLabels(request.getComplianceCase().getCustomFieldsMap())
                        .build())
                .build();
        RpcPort<BrowseJournalEntriesRequest, BrowseJournalEntriesResponse> port0 = RpcPort.adapt(ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseJournalEntries);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = LintControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintControlsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.compliance.v1.ControlValidationService/LintControls", outbound1);
        var reply1 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintControls(command1.payload());

        var outbound2 = SubmitExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(reply1.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.risk.v1.ExposureApprovalService/SubmitExposure", outbound2);
        var reply2 = riskExposureApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitExposure(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return ApproveComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .setOutcome(ApproveComplianceCaseResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a compliance case.
     */
    public RejectComplianceCaseResponse rejectComplianceCase(RejectComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<EstimateControlResponse>();
        events.publishEvent(new RejectComplianceCaseStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        List<CloneIncidentResponse> results1 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> CloneIncidentRequest.newBuilder()
                .setContext(request.getContext())
                .setIncident(Incident.newBuilder()
                        .addComments(IncidentComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .setBody(element.getBody())
                                .setCreatedAt(element.getCreatedAt())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(itsmIncidentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneIncident(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? CloneIncidentResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return RejectComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setSubject(request.getComplianceCase().getSubject())
                        .build())
                .build();
    }

    /**
     * Escalates a compliance case.
     */
    public EscalateComplianceCaseResponse escalateComplianceCase(EscalateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var futures0 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> ExplainControlCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainControlCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ExplainControlCalculationResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<PreviewNotificationResponse>();
        for (var element : request.getComplianceCase().getHistoryList()) {
            PreviewNotificationRequest itemRequest = PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .addHistory(NotificationStatusChange.newBuilder()
                                .setChangedAt(element.getChangedAt())
                                .setFromStatus(element.getFromStatus())
                                .build())
                        .build())
                .build();
            results1.add(notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewNotification(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PreviewNotificationResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return EscalateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(request.getComplianceCase().getAssignee())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .build();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public ListPendingComplianceCasesResponse listPendingComplianceCases(ListPendingComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "ListPendingComplianceCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingComplianceCases", request.getPage().getPageToken());
        return ListPendingComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Delegates a compliance case.
     */
    public DelegateComplianceCaseApprovalResponse delegateComplianceCaseApproval(DelegateComplianceCaseApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateComplianceCaseApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ExplainNotificationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = notificationsNotificationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainNotificationDecision(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateComplianceCaseApproval", request.getComplianceCase().getComplianceCaseId());
        return DelegateComplianceCaseApprovalResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setPriority(reply0.getNotification().getPriority())
                        .setSubject(request.getComplianceCase().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalls a compliance case.
     */
    public RecallComplianceCaseResponse recallComplianceCase(RecallComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ScheduleControlExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ControlLifecycleService");
        var selected0 = ControlLifecycleServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scheduleControlExpiry(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ListCollectionCaseWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCollectionCaseWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getControl().getOwner().getUserId())
                        .build())
                .build()), ListCollectionCaseWorkflowsRequest.class);
        var reply1 = collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCollectionCaseWorkflows(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return RecallComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setPriority(reply0.getControl().getPriority())
                        .build())
                .setOutcome(RecallComplianceCaseResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public GetComplianceCaseApprovalStatusResponse getComplianceCaseApprovalStatus(GetComplianceCaseApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetComplianceCaseApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "GetComplianceCaseApprovalStatus");

        var outbound0 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setComplianceCaseId(request.getComplianceCaseId())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<CalculateComplianceCaseResponse>();
        events.publishEvent(new GetComplianceCaseApprovalStatusStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = FindSecretsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSecretsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getComplianceCase().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getComplianceCase().getComplianceCaseId())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> secretsSecretQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSecrets(value));
        return GetComplianceCaseApprovalStatusResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a compliance case.
     */
    public CommentComplianceCaseApprovalResponse commentComplianceCaseApproval(CommentComplianceCaseApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentComplianceCaseApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = CancelLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelLoanWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .setPhone(request.getComplianceCase().getRequester().getPhone())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getComplianceCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        RpcPort<CancelLoanWorkflowRequest, CancelLoanWorkflowResponse> port0 = RpcPort.adapt(lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cancelLoanWorkflow);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CountAttestationTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAttestationTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getLoan().getLabelsMap())
                        .setOwnerId(request.getComplianceCase().getSla().getPolicyId())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAttestationTotals(value));
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentComplianceCaseApproval", request.getComplianceCase().getComplianceCaseId());
        return CommentComplianceCaseApprovalResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setOutcome(CommentComplianceCaseApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
