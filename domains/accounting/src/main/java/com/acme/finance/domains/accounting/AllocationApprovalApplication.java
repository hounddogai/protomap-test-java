package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.ApproveAccrualRequest;
import com.acme.accounting.v1.ApproveAccrualResponse;
import com.acme.accounting.v1.ApproveAllocationRequest;
import com.acme.accounting.v1.ApproveAllocationResponse;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CommentAllocationApprovalRequest;
import com.acme.accounting.v1.CommentAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateAllocationApprovalRequest;
import com.acme.accounting.v1.DelegateAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.GetAllocationApprovalStatusRequest;
import com.acme.accounting.v1.GetAllocationApprovalStatusResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.ListPendingAllocationsRequest;
import com.acme.accounting.v1.ListPendingAllocationsResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.RecallAllocationRequest;
import com.acme.accounting.v1.RecallAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.SubmitAllocationRequest;
import com.acme.accounting.v1.SubmitAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyCondition;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.collections.v1.ApproveCollectionCaseRequest;
import com.acme.collections.v1.ApproveCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageCondition;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.SchedulePremiumReportRequest;
import com.acme.insurance.v1.SchedulePremiumReportResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLine;
import com.acme.invoicing.v1.EInvoiceLineTax;
import com.acme.invoicing.v1.EscalateEInvoiceRequest;
import com.acme.invoicing.v1.EscalateEInvoiceResponse;
import com.acme.invoicing.v1.ListPendingEInvoicesRequest;
import com.acme.invoicing.v1.ListPendingEInvoicesResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes allocations through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AllocationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(AllocationApprovalApplication.class);

    private final AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking;

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking;

    private final AccountingRepository repository;

    private final Executor rpcExecutor;

    public AllocationApprovalApplication(
            AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking,
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking,
            AccountingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingAccrualApprovalServiceBlocking = accountingAccrualApprovalServiceBlocking;
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.accountingChartOfAccountsServiceBlocking = accountingChartOfAccountsServiceBlocking;
        this.accountingReconciliationValidationServiceBlocking = accountingReconciliationValidationServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.invoicingEInvoiceApprovalServiceBlocking = invoicingEInvoiceApprovalServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits an allocation.
     */
    public SubmitAllocationResponse submitAllocation(SubmitAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var results0 = new ArrayList<AnnotateRetentionPolicyVersionResponse>();
        for (var element : request.getAllocation().getConditionsList()) {
            AnnotateRetentionPolicyVersionRequest itemRequest = AnnotateRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .addConditions(RetentionPolicyCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build();
            results0.add(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateRetentionPolicyVersion(itemRequest));
        }
        var reply0 = results0.isEmpty() ? AnnotateRetentionPolicyVersionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ApproveAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ApproveAccrualRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> accountingAccrualApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveAccrual(value));

        var outbound2 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getAllocation().getAllocationId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input2 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(request.getAllocation().getOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .setEtag(request.getAllocation().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input3 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitAllocation", request.getAllocation().getName());
        return SubmitAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .putAllParameters(request.getAllocation().getParametersMap())
                        .build())
                .setOutcome(SubmitAllocationResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Approves an allocation.
     */
    public ApproveAllocationResponse approveAllocation(ApproveAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .addLines(AuditTrailLine.newBuilder()
                                .setAmount(request.getAllocation().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(outbound0);

        var outbound1 = ListChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListChartOfAccountsesRequest.Filter.newBuilder()
                        .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> accountingChartOfAccountsServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listChartOfAccountses(value));

        var outbound2 = EscalateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateEInvoiceRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAuditTrail().getAudit().getEtag())
                                .build())
                        .addLines(EInvoiceLine.newBuilder()
                                .setTax(EInvoiceLineTax.newBuilder()
                                        .setAmount(request.getAllocation().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> invoicingEInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateEInvoice(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveAllocation", request.getAllocation().getAllocationId());
        return ApproveAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .setEffective(request.getAllocation().getEffective())
                        .build())
                .setOutcome(ApproveAllocationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists allocations a page at a time.
     */
    public ListPendingAllocationsResponse listPendingAllocations(ListPendingAllocationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingAllocations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getContext().getTraceId(), "ListPendingAllocations");

        var outbound0 = DelegateChartOfAccountsApprovalRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<DelegateChartOfAccountsApprovalRequest, DelegateChartOfAccountsApprovalResponse>() {
            @Override
            public DelegateChartOfAccountsApprovalResponse apply(DelegateChartOfAccountsApprovalRequest input) {
                return accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateChartOfAccountsApproval(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingAllocations", request.getPage().getPageToken());
        return ListPendingAllocationsResponse.newBuilder()
                .addAllocations(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .setName(reply0.getChartOfAccounts().getName())
                        .build())
                .build();
    }

    /**
     * Delegates an allocation.
     */
    public DelegateAllocationApprovalResponse delegateAllocationApproval(DelegateAllocationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateAllocationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = SharePremiumReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SharePremiumReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(switch (request.getAllocation().getActionCase()) {
                    case BLOCK -> request.getAllocation().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply0 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sharePremiumReport(outbound0);

        var outbound1 = SchedulePremiumReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getAllocation().getBlock().getReason())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.PremiumReportingService/SchedulePremiumReport", outbound1);
        var reply1 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePremiumReport(command1.payload());

        var outbound2 = CalculateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command2 = new DispatchCommand<>("acme.accounting.v1.AllocationCalculationService/CalculateAllocation", outbound2);
        var reply2 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAllocation(command2.payload());

        var outbound3 = BeanCopies.copy(RecallAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .addLines(AccrualLine.newBuilder()
                                .setTax(AccrualLineTax.newBuilder()
                                        .setAmount(reply2.getAllocation().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RecallAccrualRequest.class);
        var reply3 = accountingAccrualApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallAccrual(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateAllocationApproval", request.getAllocation().getAllocationId());
        return DelegateAllocationApprovalResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getAccrual().getAudit().getCreatedAt())
                                .setCreatedBy(reply2.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .setPriority(request.getAllocation().getPriority())
                        .build())
                .setOutcome(DelegateAllocationApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls an allocation.
     */
    public RecallAllocationResponse recallAllocation(RecallAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        List<RejectCoverageResponse> results0 = request.getAllocation().getConditionsList().stream()
                .map(element -> RejectCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .addConditions(CoverageCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCoverage(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RejectCoverageResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPendingEInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingEInvoicesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAllocation().getParametersMap())
                        .setOwnerId(reply0.getCoverage().getOwner().getUserId())
                        .build())
                .build();
        var adapter1 = new Function<ListPendingEInvoicesRequest, ListPendingEInvoicesResponse>() {
            @Override
            public ListPendingEInvoicesResponse apply(ListPendingEInvoicesRequest input) {
                return invoicingEInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingEInvoices(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = LintReconciliationsRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAllocation().getAudit().getEtag())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<LintReconciliationsRequest, LintReconciliationsResponse>() {
            @Override
            public LintReconciliationsResponse apply(LintReconciliationsRequest input) {
                return accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintReconciliations(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallAllocation", request.getAllocation().getAllocationId());
        return RecallAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setName(request.getAllocation().getName())
                        .build())
                .setOutcome(RecallAllocationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an allocation.
     */
    public GetAllocationApprovalStatusResponse getAllocationApprovalStatus(GetAllocationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAllocationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getAllocationId(), "GetAllocationApprovalStatus");

        var outbound0 = ApproveCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.collections.v1.CollectionCaseApprovalService/ApproveCollectionCase", outbound0);
        var reply0 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveCollectionCase(command0.payload());

        var outbound1 = SimulateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCollectionCase().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getCollectionCase().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getCollectionCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getCollectionCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(reply0.getCollectionCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateAllocation(outbound1);
        return GetAllocationApprovalStatusResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollectionCase().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getCollectionCase().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Comments an allocation.
     */
    public CommentAllocationApprovalResponse commentAllocationApproval(CommentAllocationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentAllocationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = ValidateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getAllocation().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateReconciliation(outbound0), rpcExecutor);

        var outbound1 = DelegateChartOfAccountsApprovalRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateChartOfAccountsApproval(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentAllocationApproval", request.getAllocation().getAllocationId());
        return CommentAllocationApprovalResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setEffective(request.getAllocation().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
