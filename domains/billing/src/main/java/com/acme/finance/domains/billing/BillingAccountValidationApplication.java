package com.acme.finance.domains.billing;

import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.ExplainBillingAccountDecisionRequest;
import com.acme.billing.v1.ExplainBillingAccountDecisionResponse;
import com.acme.billing.v1.ExplainInvoiceCalculationRequest;
import com.acme.billing.v1.ExplainInvoiceCalculationResponse;
import com.acme.billing.v1.GetBillingAccountRulesRequest;
import com.acme.billing.v1.GetBillingAccountRulesResponse;
import com.acme.billing.v1.GetInvoiceRatesRequest;
import com.acme.billing.v1.GetInvoiceRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.ValidateBillingAccountRequest;
import com.acme.billing.v1.ValidateBillingAccountResponse;
import com.acme.budgeting.v1.ValidateVarianceRequest;
import com.acme.budgeting.v1.ValidateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
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
import org.springframework.stereotype.Service;

/**
 * Checks billing accounts against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BillingAccountValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(BillingAccountValidationApplication.class);

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceFutureStub billingInvoiceCalculationServiceFuture;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final BillingAccountValidationRequestMapper mapper;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    public BillingAccountValidationApplication(
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceFutureStub billingInvoiceCalculationServiceFuture,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            BillingAccountValidationRequestMapper mapper,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking) {
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceCalculationServiceFuture = billingInvoiceCalculationServiceFuture;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
    }

    /**
     * Validates a billing account.
     */
    public ValidateBillingAccountResponse validateBillingAccount(ValidateBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = GetInvoiceRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getBillingAccount().getHolder().getInvoiceId())
                .build();
        var pending0 = billingInvoiceCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateBillingAccount", request.getBillingAccount().getBillingAccountId());
        return ValidateBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setCurrencyCode(request.getBillingAccount().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Previews a billing account.
     */
    public PreviewBillingAccountResponse previewBillingAccount(PreviewBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = BeanCopies.copy(GetInvoiceRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getBillingAccount().getHolder().getInvoiceId())
                .build(), GetInvoiceRatesRequest.class);
        var reply0 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(outbound0);

        var outbound1 = LintDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintDocumentsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getInvoice().getBillingAddress().getLinesList())
                                .build())
                        .setCard(DocumentCardSettlement.newBuilder()
                                .setLast4(switch (request.getBillingAccount().getFundingCase()) {
                                    case CARD -> request.getBillingAccount().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (request.getBillingAccount().getFundingCase()) {
                                    case CARD -> request.getBillingAccount().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintDocuments(outbound1);

        var outbound2 = BeanCopies.copy(ValidateVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getInvoice().getCounterparty().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getBillingAccount().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ValidateVarianceRequest.class);
        var reply2 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVariance(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewBillingAccount", request.getBillingAccount().getBillingAccountId());
        return PreviewBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getVariance().getAudit().getCreatedBy())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getInvoice().getCounterparty().getDisplayName())
                                .setNumber(reply1.getDocument().getNumber())
                                .build())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a billing account.
     */
    public CheckBillingAccountEligibilityResponse checkBillingAccountEligibility(CheckBillingAccountEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckBillingAccountEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = (RecalculateInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoices(outbound0);

        var outbound1 = mapper.checkBillingAccountEligibilityStage1(request);
        var reply1 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckBillingAccountEligibility", request.getBillingAccount().getBillingAccountId());
        return CheckBillingAccountEligibilityResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a billing account.
     */
    public NormalizeBillingAccountResponse normalizeBillingAccount(NormalizeBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = EstimateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setHolderName(request.getBillingAccount().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateInvoice(outbound0), rpcExecutor);

        var outbound1 = CheckPostingBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBank(PostingBatchBankSettlement.newBuilder()
                                .setIban(request.getBillingAccount().getBank().getIban())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPostingBatchEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeBillingAccount", request.getBillingAccount().getBillingAccountId());
        return NormalizeBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setCurrencyCode(request.getBillingAccount().getCurrencyCode())
                        .build())
                .setOutcome(NormalizeBillingAccountResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains a billing account.
     */
    public ExplainBillingAccountDecisionResponse explainBillingAccountDecision(ExplainBillingAccountDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBillingAccountDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        List<SimulateInvoiceResponse> results0 = request.getBillingAccount().getLimitsList().stream()
                .map(element -> SimulateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBillingAccount().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .build())
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoice(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SimulateInvoiceResponse.getDefaultInstance()
                : results0.getLast();

        List<EstimateInvoiceResponse> results1 = request.getBillingAccount().getLimitsList().stream()
                .map(element -> EstimateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateInvoice(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EstimateInvoiceResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = NormalizeStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            NormalizeStandingOrderResponse fetch(NormalizeStandingOrderRequest input) {
                return bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeStandingOrder(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = GetInvoiceRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getBillingAccount().getHolder().getInvoiceId())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(value));
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBillingAccountDecision", request.getBillingAccount().getBillingAccountId());
        return ExplainBillingAccountDecisionResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getStandingOrder().getAudit().getCreatedBy())
                                .setUpdatedBy(reply3.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(request.getBillingAccount().getContact())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply3.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Scores a billing account.
     */
    public ScoreBillingAccountResponse scoreBillingAccount(ScoreBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = SimulateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getBillingAccount().getFundingCase()) {
                                    case BANK -> request.getBillingAccount().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoice(outbound0);

        var outbound1 = ExplainInvoiceCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setHolderName(request.getBillingAccount().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceCalculation(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(GetInvoiceRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getBillingAccount().getHolder().getInvoiceId())
                .build()), GetInvoiceRatesRequest.class);
        var reply2 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreBillingAccount", request.getBillingAccount().getBillingAccountId());
        return ScoreBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getInvoice().getCounterparty().getEmail())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setBreakdown(ScoreBillingAccountResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lints a billing account.
     */
    public LintBillingAccountsResponse lintBillingAccounts(LintBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = SimulateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        Function<SimulateInvoiceRequest, SimulateInvoiceResponse> operation0 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateInvoice;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = GetInvoiceRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceId(request.getBillingAccount().getHolder().getInvoiceId())
                .build();
        final class Stage1Adapter {
            GetInvoiceRatesResponse fetch(GetInvoiceRatesRequest input) {
                return billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceRates(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = BeanCopies.copy(BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getInvoice().getAudit().getCreatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setOwnerId(reply0.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(reply0.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build(), BrowseSupplierContractsRequest.class);
        var reply2 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintBillingAccounts", request.getBillingAccount().getBillingAccountId());
        return LintBillingAccountsResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .setStatus(request.getBillingAccount().getStatus())
                        .build())
                .setOutcome(LintBillingAccountsResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a billing account.
     */
    public GetBillingAccountRulesResponse getBillingAccountRules(GetBillingAccountRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBillingAccountRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getBillingAccountId(), "GetBillingAccountRules");

        var outbound0 = RecalculateInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(InvoiceLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.InvoiceCalculationService");
        var selected0 = InvoiceCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.recalculateInvoices(outbound0);
        return GetBillingAccountRulesResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .build())
                .build();
    }
}
