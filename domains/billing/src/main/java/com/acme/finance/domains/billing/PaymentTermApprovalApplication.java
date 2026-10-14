package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApprovePaymentTermRequest;
import com.acme.billing.v1.ApprovePaymentTermResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.CountInvoicesRequest;
import com.acme.billing.v1.CountInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.DelegatePaymentTermApprovalRequest;
import com.acme.billing.v1.DelegatePaymentTermApprovalResponse;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.GetPaymentTermApprovalStatusRequest;
import com.acme.billing.v1.GetPaymentTermApprovalStatusResponse;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.ListBillingAccountWorkflowsRequest;
import com.acme.billing.v1.ListBillingAccountWorkflowsResponse;
import com.acme.billing.v1.ListInvoiceWorkflowsRequest;
import com.acme.billing.v1.ListInvoiceWorkflowsResponse;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.ListPendingBillingAccountsResponse;
import com.acme.billing.v1.ListPendingPaymentTermsRequest;
import com.acme.billing.v1.ListPendingPaymentTermsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermDiscountAction;
import com.acme.billing.v1.PaymentTermLifecycleServiceGrpc;
import com.acme.billing.v1.RecallPaymentTermRequest;
import com.acme.billing.v1.RecallPaymentTermResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.StartBillingAccountWorkflowRequest;
import com.acme.billing.v1.StartBillingAccountWorkflowResponse;
import com.acme.billing.v1.SubmitPaymentTermRequest;
import com.acme.billing.v1.SubmitPaymentTermResponse;
import com.acme.billing.v1.SuspendPaymentTermRequest;
import com.acme.billing.v1.SuspendPaymentTermResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceBlockAction;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.SimulatePreferenceRequest;
import com.acme.customers.v1.SimulatePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.CommentSupplierContractApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierContractApprovalResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
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
 * Routes payment terms through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PaymentTermApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentTermApprovalApplication.class);

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking;

    private final CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceBlockingStub billingCreditNoteLifecycleServiceBlocking;

    private final DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceBlockingStub billingInvoiceWorkflowServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final PaymentTermLifecycleServiceGrpc.PaymentTermLifecycleServiceBlockingStub billingPaymentTermLifecycleServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    public PaymentTermApprovalApplication(
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking,
            CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceBlockingStub billingCreditNoteLifecycleServiceBlocking,
            DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceBlockingStub billingInvoiceWorkflowServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            PaymentTermLifecycleServiceGrpc.PaymentTermLifecycleServiceBlockingStub billingPaymentTermLifecycleServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking) {
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingBillingAccountWorkflowServiceBlocking = billingBillingAccountWorkflowServiceBlocking;
        this.billingCreditNoteLifecycleServiceBlocking = billingCreditNoteLifecycleServiceBlocking;
        this.billingDunningReportingServiceBlocking = billingDunningReportingServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.billingInvoiceWorkflowServiceBlocking = billingInvoiceWorkflowServiceBlocking;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.billingPaymentTermLifecycleServiceBlocking = billingPaymentTermLifecycleServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
    }

    /**
     * Submits a payment term.
     */
    public SubmitPaymentTermResponse submitPaymentTerm(SubmitPaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = ListPendingBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingBillingAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentTerm().getAudit().getCreatedAt())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingBillingAccounts(outbound0), rpcExecutor);

        var outbound1 = ShareDunningReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareDunningReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getPaymentTerm().getBlock().getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDunningReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CancelSupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPaymentTerm().getAudit().getCreatedBy())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply1.getDunning().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.suppliers.v1.SupplierWorkflowService");
        var selected2 = SupplierWorkflowServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.cancelSupplierWorkflow(outbound2);

        var outbound3 = ListBillingAccountWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBillingAccountWorkflowsRequest.Filter.newBuilder()
                        .setRegion(reply2.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> billingBillingAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBillingAccountWorkflows(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return SubmitPaymentTermResponse.newBuilder()
                .setOutcome(SubmitPaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDunning().getAudit().getCreatedBy())
                                .build())
                        .putAllParameters(request.getPaymentTerm().getParametersMap())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a payment term.
     */
    public ApprovePaymentTermResponse approvePaymentTerm(ApprovePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = SimulatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .setBlock(PreferenceBlockAction.newBuilder()
                                .setNotifyEmail(switch (request.getPaymentTerm().getActionCase()) {
                                    case BLOCK -> request.getPaymentTerm().getBlock().getNotifyEmail();
                                    default -> "";
                                })
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePreference(outbound0);

        var outbound1 = ComparePaymentTermOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setDiscount(PaymentTermDiscountAction.newBuilder()
                                .setAmount(reply0.getPreference().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ComparePaymentTermOptionsRequest, ComparePaymentTermOptionsResponse>() {
            @Override
            public ComparePaymentTermOptionsResponse apply(ComparePaymentTermOptionsRequest input) {
                return billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePaymentTermOptions(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = GenerateDunningReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(switch (reply0.getPreference().getActionCase()) {
                    case BLOCK -> reply0.getPreference().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply2 = billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDunningReport(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return ApprovePaymentTermResponse.newBuilder()
                .setOutcome(ApprovePaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPaymentTerm().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getPreference().getOwner().getUserId())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }

    /**
     * Lists payment terms a page at a time.
     */
    public ListPendingPaymentTermsResponse listPendingPaymentTerms(ListPendingPaymentTermsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPaymentTerms")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getContext().getTraceId(), "ListPendingPaymentTerms");

        var outbound0 = ScheduleCreditNoteExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = billingCreditNoteLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCreditNoteExpiry(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPaymentTerms", request.getPage().getPageToken());
        return ListPendingPaymentTermsResponse.newBuilder()
                .addPaymentTerms(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a payment term.
     */
    public DelegatePaymentTermApprovalResponse delegatePaymentTermApproval(DelegatePaymentTermApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegatePaymentTermApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = CommentSupplierContractApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .addLines(SupplierContractLine.newBuilder()
                                .setTax(SupplierContractLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentSupplierContractApproval(value));

        var outbound1 = ListInvoiceWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListInvoiceWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSupplierContract().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getSupplierContract().getCounterparty().getSupplierId())
                        .setRegion(reply0.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<ListInvoiceWorkflowsRequest, ListInvoiceWorkflowsResponse>() {
            @Override
            public ListInvoiceWorkflowsResponse apply(ListInvoiceWorkflowsRequest input) {
                return billingInvoiceWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listInvoiceWorkflows(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = StartBillingAccountWorkflowRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setLast4(switch (reply0.getSupplierContract().getSettlementCase()) {
                                    case CARD -> reply0.getSupplierContract().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getSupplierContract().getCounterparty().getEmail())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = billingBillingAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startBillingAccountWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegatePaymentTermApproval", request.getPaymentTerm().getPaymentTermId());
        return DelegatePaymentTermApprovalResponse.newBuilder()
                .setOutcome(DelegatePaymentTermApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setEffective(request.getPaymentTerm().getEffective())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a payment term.
     */
    public RecallPaymentTermResponse recallPaymentTerm(RecallPaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = SuspendPaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SuspendPaymentTermRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setPaymentTermId(request.getPaymentTerm().getPaymentTermId())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> billingPaymentTermLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suspendPaymentTerm(value)).orElseThrow();

        var outbound1 = JsonCodec.roundTrip(ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(reply0.getPaymentTerm().getBlock().getReason())
                .build(), ScheduleFiscalPeriodExportRequest.class);
        var reply1 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound1);

        var outbound2 = CountInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountInvoicesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getSummaryMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        Function<CountInvoicesRequest, CountInvoicesResponse> operation2 = billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countInvoices;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = BeanCopies.copy(ValidateVarianceImportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateVarianceImportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ValidateVarianceImportRequest.class);
        var reply3 = budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVarianceImport(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPaymentTerm", request.getPaymentTerm().getName());
        return RecallPaymentTermResponse.newBuilder()
                .setOutcome(RecallPaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .addAllScopes(reply0.getPaymentTerm().getScopesList())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a payment term.
     */
    public GetPaymentTermApprovalStatusResponse getPaymentTermApprovalStatus(GetPaymentTermApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPaymentTermApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "GetPaymentTermApprovalStatus");

        var outbound0 = BeanCopies.copy(ResumePaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), ResumePaymentPromiseWorkflowRequest.class);
        var reply0 = collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumePaymentPromiseWorkflow(outbound0);
        return GetPaymentTermApprovalStatusResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getPaymentPromise().getOrganizer().getEmail())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
