package com.acme.finance.domains.billing;

import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CheckDunningEligibilityRequest;
import com.acme.billing.v1.CheckDunningEligibilityResponse;
import com.acme.billing.v1.CloneBillingAccountRequest;
import com.acme.billing.v1.CloneBillingAccountResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CreateBillingAccountRequest;
import com.acme.billing.v1.CreateBillingAccountResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.GetBillingAccountRequest;
import com.acme.billing.v1.GetBillingAccountResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListBillingAccountsRequest;
import com.acme.billing.v1.ListBillingAccountsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.billing.v1.ResolveBillingAccountsRequest;
import com.acme.billing.v1.ResolveBillingAccountsResponse;
import com.acme.billing.v1.ScorePaymentTermRequest;
import com.acme.billing.v1.ScorePaymentTermResponse;
import com.acme.billing.v1.UpdateBillingAccountRequest;
import com.acme.billing.v1.UpdateBillingAccountResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceStatusChange;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages billing accounts: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class BillingAccountApplication {
    private static final Logger log = LoggerFactory.getLogger(BillingAccountApplication.class);

    private final BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking;

    private final DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final PaymentTermValidationServiceGrpc.PaymentTermValidationServiceBlockingStub billingPaymentTermValidationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final BillingRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public BillingAccountApplication(
            BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking,
            DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            PaymentTermValidationServiceGrpc.PaymentTermValidationServiceBlockingStub billingPaymentTermValidationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ApplicationEventPublisher events,
            BillingRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.billingBillingAccountQueryServiceBlocking = billingBillingAccountQueryServiceBlocking;
        this.billingDunningValidationServiceBlocking = billingDunningValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.billingPaymentTermValidationServiceBlocking = billingPaymentTermValidationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.events = events;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    public record BatchCreateBillingAccountsStage2Requested(QuotePaymentTermRequest request, CompletableFuture<QuotePaymentTermResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a billing account.
     */
    public CreateBillingAccountResponse createBillingAccount(CreateBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var builder0 = BrowseInvoicesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseInvoicesRequest.Filter.newBuilder()
                .setCreatedAfter(request.getBillingAccount().getAudit().getUpdatedAt())
                .putAllLabels(request.getBillingAccount().getLabelsMap())
                .setOwnerId(request.getBillingAccount().getBillingAccountId())
                .build());
        var outbound0 = builder0.build();
        var reply0 = billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseInvoices(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getHolder().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateBillingAccount", request.getBillingAccount().getHolder().getInvoiceId());
        return CreateBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setContact(request.getBillingAccount().getContact())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a billing account.
     */
    public GetBillingAccountResponse getBillingAccount(GetBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getBillingAccountId(), "GetBillingAccount");

        var outbound0 = ScorePaymentTermRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> billingPaymentTermValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePaymentTerm(value));
        return GetBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .build())
                .build();
    }

    /**
     * Changes a billing account.
     */
    public UpdateBillingAccountResponse updateBillingAccount(UpdateBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = CheckDunningEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getBillingAccount().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> billingDunningValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkDunningEligibility(value));

        var outbound1 = ScoreBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBillingAccount().getAudit().getCreatedBy())
                                .setEtag(reply0.getDunning().getAudit().getEtag())
                                .setUpdatedAt(reply0.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudgetLine(value));
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateBillingAccount", request.getBillingAccount().getBillingAccountId());
        return UpdateBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDunning().getAudit().getEtag())
                                .build())
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDunning().getRequester().getEmail())
                                .build())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public ListBillingAccountsResponse listBillingAccounts(ListBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "ListBillingAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBillingAccounts", request.getPage().getPageToken());
        return ListBillingAccountsResponse.getDefaultInstance();
    }

    /**
     * Returns several billing accounts at once.
     */
    public BatchGetBillingAccountsResponse batchGetBillingAccounts(BatchGetBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "BatchGetBillingAccounts");

        var outbound0 = PreviewVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewVarianceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addHistory(VarianceStatusChange.newBuilder()
                                .setChangedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVariance(outbound0);
        return BatchGetBillingAccountsResponse.newBuilder()
                .addBillingAccounts(BillingAccount.newBuilder()
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of billing accounts and answers once at the end.
     */
    public BatchCreateBillingAccountsResponse batchCreateBillingAccounts(BatchCreateBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveBillingAccount(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = JsonCodec.roundTrip(BrowseRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getAudit().getUpdatedAt())
                        .putAllLabels(request.getRecord().getLabelsMap())
                        .setOwnerId(request.getRecord().getHolder().getInvoiceId())
                        .build())
                .build(), BrowseRiskRegistersRequest.class);
        var reply0 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseRiskRegisters(outbound0);

        var outbound1 = CountBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBillingAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getAudit().getUpdatedAt())
                        .putAllLabels(request.getRecord().getLabelsMap())
                        .setOwnerId(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getHolder().getInvoiceId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = billingBillingAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBillingAccounts(outbound1);

        var outbound2 = QuotePaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<QuotePaymentTermResponse>();
        events.publishEvent(new BatchCreateBillingAccountsStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = CalculateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getRecord().getAudit().getUpdatedAt())
                                .build())
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setIban(switch (request.getRecord().getFundingCase()) {
                                    case BANK -> request.getRecord().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoice(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateBillingAccounts", request.getHeader().getSource());
        return BatchCreateBillingAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Creates a billing account.
     */
    public CloneBillingAccountResponse cloneBillingAccount(CloneBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ListSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBillingAccount().getAudit().getUpdatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setOwnerId(request.getBillingAccount().getExternalAccountId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSuppliers(outbound0), rpcExecutor);

        var outbound1 = ResolveBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingBillingAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBillingAccounts(outbound1), rpcExecutor);

        var outbound2 = ScorePaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBillingAccount().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> billingPaymentTermValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePaymentTerm(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneBillingAccount", request.getBillingAccount().getBillingAccountId());
        return CloneBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getPaymentTerm().getAudit().getCreatedBy())
                                .build())
                        .setOpenedAt(request.getBillingAccount().getOpenedAt())
                        .setStatus(request.getBillingAccount().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
