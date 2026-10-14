package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.ApproveAccrualRequest;
import com.acme.accounting.v1.ApproveAccrualResponse;
import com.acme.accounting.v1.BrowseChartOfAccountsesRequest;
import com.acme.accounting.v1.BrowseChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.CommentAccrualApprovalRequest;
import com.acme.accounting.v1.CommentAccrualApprovalResponse;
import com.acme.accounting.v1.CountAccrualsRequest;
import com.acme.accounting.v1.CountAccrualsResponse;
import com.acme.accounting.v1.DelegateAccrualApprovalRequest;
import com.acme.accounting.v1.DelegateAccrualApprovalResponse;
import com.acme.accounting.v1.EscalateAccrualRequest;
import com.acme.accounting.v1.EscalateAccrualResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExportChartOfAccountsesRequest;
import com.acme.accounting.v1.ExportChartOfAccountsesResponse;
import com.acme.accounting.v1.FindAccrualsRequest;
import com.acme.accounting.v1.FindAccrualsResponse;
import com.acme.accounting.v1.GetAccrualApprovalStatusRequest;
import com.acme.accounting.v1.GetAccrualApprovalStatusResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.SubmitAccrualRequest;
import com.acme.accounting.v1.SubmitAccrualResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverCalculationServiceGrpc;
import com.acme.drivers.v1.QuoteDriverRequest;
import com.acme.drivers.v1.QuoteDriverResponse;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.GetUserRulesRequest;
import com.acme.identity.v1.GetUserRulesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
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
 * Routes accruals through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AccrualApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(AccrualApprovalApplication.class);

    private final AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking;

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking;

    private final BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceBlockingStub budgetingBudgetLineWorkflowServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final DriverCalculationServiceGrpc.DriverCalculationServiceBlockingStub driversDriverCalculationServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final AccountingRepository repository;

    private final Executor rpcExecutor;

    public AccrualApprovalApplication(
            AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking,
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking,
            BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceBlockingStub budgetingBudgetLineWorkflowServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            DriverCalculationServiceGrpc.DriverCalculationServiceBlockingStub driversDriverCalculationServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            AccountingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingAccrualQueryServiceBlocking = accountingAccrualQueryServiceBlocking;
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.accountingChartOfAccountsQueryServiceBlocking = accountingChartOfAccountsQueryServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.billingCreditNoteApprovalServiceBlocking = billingCreditNoteApprovalServiceBlocking;
        this.budgetingBudgetLineWorkflowServiceBlocking = budgetingBudgetLineWorkflowServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.driversDriverCalculationServiceBlocking = driversDriverCalculationServiceBlocking;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits an accrual.
     */
    public SubmitAccrualResponse submitAccrual(SubmitAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = ExportChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportChartOfAccountsesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAccrual().getAudit().getCreatedAt())
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = accountingChartOfAccountsQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportChartOfAccountses(outbound0);
        var reply0 = ExportChartOfAccountsesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = GetUserRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getDetails().getApprover().getUserId())
                .build();
        var reply1 = RpcSupport.retryRead(() -> identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getUserRules(outbound1));

        var outbound2 = (RejectCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getUser().getPrimaryAddress().getCountryCode())
                                .setLocality(request.getAccrual().getBillingAddress().getLocality())
                                .build())
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setHolderName(request.getAccrual().getCard().getHolderName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCreditNote(outbound2);

        var outbound3 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getAccrual().getAccrualId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input3 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getDetails().getApprover())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAccrual().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAccrual().getAudit().getCreatedBy())
                                .setEtag(request.getAccrual().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input4 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitAccrual", request.getAccrual().getNumber());
        return SubmitAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .setCounterparty(AllocationRef.newBuilder()
                                .setDisplayName(reply2.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .setNumber(reply2.getCreditNote().getNumber())
                        .setTotal(request.getAccrual().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Approves an accrual.
     */
    public ApproveAccrualResponse approveAccrual(ApproveAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = JsonCodec.roundTrip(ShareAttestationReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAttestationReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build(), ShareAttestationReportRequest.class);
        var reply0 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAttestationReport(outbound0);

        var outbound1 = SignalBudgetLineWorkflowRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setIban(switch (request.getAccrual().getSettlementCase()) {
                                    case BANK -> request.getAccrual().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getAccrual().getBillingAddress().getRecipient())
                                .setRegion(request.getAccrual().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetLineWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalBudgetLineWorkflow(outbound1);

        var outbound2 = (ShareUnderwritingReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ShareUnderwritingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply2 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareUnderwritingReport(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveAccrual", request.getAccrual().getAccrualId());
        return ApproveAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBudgetLine().getAudit().getCreatedBy())
                                .setEtag(reply0.getAttestation().getAudit().getEtag())
                                .build())
                        .putAllMetadata(request.getAccrual().getMetadataMap())
                        .build())
                .setOutcome(ApproveAccrualResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates an accrual.
     */
    public EscalateAccrualResponse escalateAccrual(EscalateAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = SubmitChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getAccrual().getBillingAddress().getCountryCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitChartOfAccounts(value)).orElseThrow();

        var outbound1 = RpcSupport.unpack(Any.pack(BrowseChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseChartOfAccountsesRequest.Filter.newBuilder()
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build()), BrowseChartOfAccountsesRequest.class);
        var reply1 = accountingChartOfAccountsQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseChartOfAccountses(outbound1);

        var outbound2 = FindAccrualsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccrualsRequest.Filter.newBuilder()
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAccruals(outbound2));
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateAccrual", request.getAccrual().getAccrualId());
        return EscalateAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .setNotes(request.getAccrual().getNotes())
                        .build())
                .setOutcome(EscalateAccrualResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates an accrual.
     */
    public DelegateAccrualApprovalResponse delegateAccrualApproval(DelegateAccrualApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateAccrualApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = EstimateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateReconciliationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getAccrual().getBillingAddress().getLinesList())
                                .setPostalCode(request.getAccrual().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setDisplayName(request.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<EstimateReconciliationRequest, EstimateReconciliationResponse> operation0 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateReconciliation;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateAccrualApproval", request.getAccrual().getAccrualId());
        return DelegateAccrualApprovalResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getReconciliation().getAudit().getUpdatedBy())
                                .build())
                        .setStatus(request.getAccrual().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalls an accrual.
     */
    public RecallAccrualResponse recallAccrual(RecallAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = LintAccrualsRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAccrual().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getAccrual().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getAccrual().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAccruals(outbound0), rpcExecutor);

        var outbound1 = ValidateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setNumber(request.getAccrual().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditEvent(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallAccrual", request.getAccrual().getAccrualId());
        return RecallAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setTotal(request.getAccrual().getTotal())
                        .build())
                .setOutcome(RecallAccrualResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an accrual.
     */
    public GetAccrualApprovalStatusResponse getAccrualApprovalStatus(GetAccrualApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAccrualApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "GetAccrualApprovalStatus");

        var outbound0 = (PreviewAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()).toBuilder().build();
        var reply0 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccrual(outbound0);
        return GetAccrualApprovalStatusResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setCounterparty(AllocationRef.newBuilder()
                                .setVersion(reply0.getAccrual().getCounterparty().getVersion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Comments an accrual.
     */
    public CommentAccrualApprovalResponse commentAccrualApproval(CommentAccrualApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentAccrualApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = GenerateForecastReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateForecastReport(outbound0), rpcExecutor);

        var outbound1 = CountAccrualsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccrualsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAccrual().getIssuedAt())
                        .setOwnerId(request.getAccrual().getCounterparty().getAllocationId())
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAccruals(outbound1), rpcExecutor);

        var outbound2 = BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(outbound2), rpcExecutor);

        var outbound3 = QuoteDriverRequest.newBuilder()
                .setContext(request.getContext())
                .setDriver(Driver.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAccrual().getAudit().getUpdatedAt())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getAccrual().getBillingAddress().getLinesList())
                                .setLocality(request.getAccrual().getBillingAddress().getLocality())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending3 = CompletableFuture.supplyAsync(() -> driversDriverCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDriver(outbound3), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2, pending3).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        var reply3 = pending3.join();
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentAccrualApproval", request.getAccrual().getAccrualId());
        return CommentAccrualApprovalResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getDriver().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setOutcome(CommentAccrualApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
