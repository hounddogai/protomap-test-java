package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.ApproveChartOfAccountsRequest;
import com.acme.accounting.v1.ApproveChartOfAccountsResponse;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusRequest;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.RecallChartOfAccountsRequest;
import com.acme.accounting.v1.RecallChartOfAccountsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.accounting.v1.ScoreAccrualRequest;
import com.acme.accounting.v1.ScoreAccrualResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowRequest;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.RecallEInvoiceRequest;
import com.acme.invoicing.v1.RecallEInvoiceResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLimit;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes chart of accountses through approval, with escalation and delegation. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ChartOfAccountsApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ChartOfAccountsApprovalApplication.class);

    private final AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking;

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync;

    private final EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final AccountingRepository repository;

    private final Executor rpcExecutor;

    public ChartOfAccountsApprovalApplication(
            AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking,
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync,
            EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            AccountingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingAccrualQueryServiceBlocking = accountingAccrualQueryServiceBlocking;
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.billingBillingAccountWorkflowServiceBlocking = billingBillingAccountWorkflowServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.events = events;
        this.insuranceUnderwritingImportServiceAsync = insuranceUnderwritingImportServiceAsync;
        this.invoicingEInvoiceApprovalServiceBlocking = invoicingEInvoiceApprovalServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record DelegateChartOfAccountsApprovalStage1Requested(CommentLoanApprovalRequest request, CompletableFuture<CommentLoanApprovalResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a chart of accounts.
     */
    public SubmitChartOfAccountsResponse submitChartOfAccounts(SubmitChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = JsonCodec.roundTrip(RecalculateReconciliationsRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getChartOfAccounts().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .build())
                .build(), RecalculateReconciliationsRequest.class);
        var reply0 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateReconciliations(outbound0);

        var outbound1 = ImportUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportUnderwritingsRequest.StreamHeader.newBuilder()
                        .setSource(request.getChartOfAccounts().getChartOfAccountsId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(ImportUnderwritingsResponse.getDefaultInstance());
        var input1 = insuranceUnderwritingImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importUnderwritings(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getChartOfAccounts().getManager())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getChartOfAccounts().getAudit().getCreatedAt())
                                .setCreatedBy(request.getChartOfAccounts().getAudit().getCreatedBy())
                                .setEtag(request.getChartOfAccounts().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input2 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getParentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitChartOfAccounts", request.getChartOfAccounts().getParentId());
        return SubmitChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setName(request.getChartOfAccounts().getName())
                        .build())
                .setOutcome(SubmitChartOfAccountsResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a chart of accounts.
     */
    public ApproveChartOfAccountsResponse approveChartOfAccounts(ApproveChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = ListFiscalPeriodSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChartOfAccounts().getAudit().getUpdatedAt())
                        .putAllLabels(request.getChartOfAccounts().getAttributesMap())
                        .setOwnerId(request.getChartOfAccounts().getParentId())
                        .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFiscalPeriodSubscriptions(outbound0), rpcExecutor);

        var outbound1 = NormalizeAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getChartOfAccounts().getAudit().getEtag())
                                .setUpdatedAt(request.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAllocation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return ApproveChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getAllocation().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getAllocation().getAudit().getUpdatedBy())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .build())
                .setOutcome(ApproveChartOfAccountsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a chart of accounts.
     */
    public RejectChartOfAccountsResponse rejectChartOfAccounts(RejectChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<RecordRetentionPolicyVersionRequest, RecordRetentionPolicyVersionResponse>() {
            @Override
            public RecordRetentionPolicyVersionResponse apply(RecordRetentionPolicyVersionRequest input) {
                return auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = JsonCodec.roundTrip(ScoreAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAccrualRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getChartOfAccounts().getLocation().getLocality())
                                .setPostalCode(request.getChartOfAccounts().getLocation().getPostalCode())
                                .build())
                        .build())
                .build(), ScoreAccrualRequest.class);
        var reply1 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAccrual(outbound1);

        var outbound2 = ExplainAllocationDecisionRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ExplainAllocationDecisionRequest, ExplainAllocationDecisionResponse> operation2 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainAllocationDecision;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = ResolveTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.localization.v1.TranslationKeyQueryService");
        var serviceName3 = "acme.localization.v1" + "." + "TranslationKeyQueryService";
        var methodName3 = "Res" + "olveTranslationKeys";
        var descriptor3 = MethodDescriptor.<ResolveTranslationKeysRequest, ResolveTranslationKeysResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveTranslationKeysRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveTranslationKeysResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return RejectChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getAccrual().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setRegion(reply1.getAccrual().getBillingAddress().getRegion())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .setParentId(request.getChartOfAccounts().getParentId())
                        .build())
                .setOutcome(RejectChartOfAccountsResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Delegates a chart of accounts.
     */
    public DelegateChartOfAccountsApprovalResponse delegateChartOfAccountsApproval(DelegateChartOfAccountsApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateChartOfAccountsApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = BrowseAccrualsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccrualsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChartOfAccounts().getAudit().getUpdatedAt())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        RpcPort<BrowseAccrualsRequest, BrowseAccrualsResponse> port0 = RpcPort.adapt(accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseAccruals);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CommentLoanApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLoanApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .addLimits(LoanLimit.newBuilder()
                                .setName(request.getChartOfAccounts().getName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CommentLoanApprovalResponse>();
        events.publishEvent(new DelegateChartOfAccountsApprovalStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = RecallEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallEInvoiceRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply1.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<RecallEInvoiceRequest, RecallEInvoiceResponse>() {
            @Override
            public RecallEInvoiceResponse apply(RecallEInvoiceRequest input) {
                return invoicingEInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallEInvoice(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateChartOfAccountsApproval", request.getChartOfAccounts().getChartOfAccountsId());
        return DelegateChartOfAccountsApprovalResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .build())
                .setOutcome(DelegateChartOfAccountsApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a chart of accounts.
     */
    public RecallChartOfAccountsResponse recallChartOfAccounts(RecallChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = ApproveStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveStandingOrderRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getChartOfAccounts().getLocation().getLinesList())
                                .setPostalCode(request.getChartOfAccounts().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ApproveStandingOrderResponse fetch(ApproveStandingOrderRequest input) {
                return bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveStandingOrder(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BeanCopies.copy(PreviewAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), PreviewAllocationRequest.class);
        var reply1 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAllocation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return RecallChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setDepth(request.getChartOfAccounts().getDepth())
                        .setManager(UserRef.newBuilder()
                                .setUserId(reply0.getStandingOrder().getOrganizer().getUserId())
                                .build())
                        .build())
                .setOutcome(RecallChartOfAccountsResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a chart of accounts.
     */
    public GetChartOfAccountsApprovalStatusResponse getChartOfAccountsApprovalStatus(GetChartOfAccountsApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetChartOfAccountsApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "GetChartOfAccountsApprovalStatus");
        return GetChartOfAccountsApprovalStatusResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .build())
                .build();
    }

    /**
     * Comments a chart of accounts.
     */
    public CommentChartOfAccountsApprovalResponse commentChartOfAccountsApproval(CommentChartOfAccountsApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentChartOfAccountsApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = SubmitStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitStandingOrderRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                                .setUserId(request.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<SubmitStandingOrderRequest, SubmitStandingOrderResponse> port0 = RpcPort.adapt(bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitStandingOrder);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(AdvanceBillingAccountWorkflowRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), AdvanceBillingAccountWorkflowRequest.class);
        var reply1 = billingBillingAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceBillingAccountWorkflow(outbound1);

        var outbound2 = SimulateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getChartOfAccounts().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getBillingAccount().getAudit().getCreatedBy())
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateAllocation(value));
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentChartOfAccountsApproval", request.getChartOfAccounts().getChartOfAccountsId());
        return CommentChartOfAccountsApprovalResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(reply1.getBillingAccount().getAudit().getUpdatedBy())
                                        .build())
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(reply0.getStandingOrder().getLocation().getRecipient())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply2.getAllocation().getOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setOutcome(CommentChartOfAccountsApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }
}
