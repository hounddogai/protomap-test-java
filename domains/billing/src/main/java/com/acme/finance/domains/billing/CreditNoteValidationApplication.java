package com.acme.finance.domains.billing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.CheckCreditNoteEligibilityRequest;
import com.acme.billing.v1.CheckCreditNoteEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetCreditNoteRulesRequest;
import com.acme.billing.v1.GetCreditNoteRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceAttachment;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.LintCreditNotesRequest;
import com.acme.billing.v1.LintCreditNotesResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PreviewCreditNoteRequest;
import com.acme.billing.v1.PreviewCreditNoteResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ScoreCreditNoteRequest;
import com.acme.billing.v1.ScoreCreditNoteResponse;
import com.acme.billing.v1.ValidateCreditNoteRequest;
import com.acme.billing.v1.ValidateCreditNoteResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.FindBudgetsRequest;
import com.acme.budgeting.v1.FindBudgetsResponse;
import com.acme.budgeting.v1.SuggestBudgetsRequest;
import com.acme.budgeting.v1.SuggestBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.SummarizeControlsRequest;
import com.acme.compliance.v1.SummarizeControlsResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.helpdesk.v1.ExplainTicketCalculationRequest;
import com.acme.helpdesk.v1.ExplainTicketCalculationResponse;
import com.acme.helpdesk.v1.Ticket;
import com.acme.helpdesk.v1.TicketCalculationServiceGrpc;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.ratelimiting.v1.ExportRateLimitsRequest;
import com.acme.ratelimiting.v1.ExportRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks credit notes against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CreditNoteValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(CreditNoteValidationApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final TicketCalculationServiceGrpc.TicketCalculationServiceBlockingStub helpdeskTicketCalculationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final RateLimitQueryServiceGrpc.RateLimitQueryServiceBlockingStub ratelimitingRateLimitQueryServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public CreditNoteValidationApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            TicketCalculationServiceGrpc.TicketCalculationServiceBlockingStub helpdeskTicketCalculationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            RateLimitQueryServiceGrpc.RateLimitQueryServiceBlockingStub ratelimitingRateLimitQueryServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.complianceControlQueryServiceBlocking = complianceControlQueryServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.helpdeskTicketCalculationServiceBlocking = helpdeskTicketCalculationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.ratelimitingRateLimitQueryServiceBlocking = ratelimitingRateLimitQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a credit note.
     */
    public ValidateCreditNoteResponse validateCreditNote(ValidateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ValidateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getCreditNote().getBillingAddress().getLinesList())
                                .build())
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setLast4(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoice(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateCreditNote", request.getCreditNote().getCreditNoteId());
        return ValidateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setDueAt(request.getCreditNote().getDueAt())
                        .setNumber(reply0.getInvoice().getNumber())
                        .build())
                .build();
    }

    /**
     * Previews a credit note.
     */
    public PreviewCreditNoteResponse previewCreditNote(PreviewCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ExportRateLimitsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportRateLimitsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCreditNote().getAudit().getCreatedAt())
                        .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = ratelimitingRateLimitQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportRateLimits(outbound0);
        var reply0 = ExportRateLimitsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = JsonCodec.roundTrip(LintObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .addComments(ObligationComment.newBuilder()
                                .setCreatedAt(request.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), LintObligationsRequest.class);
        var reply1 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintObligations(outbound1);

        var outbound2 = BatchGetNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var command2 = new DispatchCommand<>("acme.notifications.v1.NotificationService/BatchGetNotifications", outbound2);
        var reply2 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetNotifications(command2.payload());

        var outbound3 = ExplainTicketCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setTicket(Ticket.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply3 = helpdeskTicketCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainTicketCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewCreditNote", request.getCreditNote().getCreditNoteId());
        return PreviewCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNumber(request.getCreditNote().getNumber())
                        .build())
                .setOutcome(PreviewCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a credit note.
     */
    public CheckCreditNoteEligibilityResponse checkCreditNoteEligibility(CheckCreditNoteEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckCreditNoteEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = SuggestBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBudgetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCreditNote().getAudit().getCreatedAt())
                        .setOwnerId(request.getCreditNote().getCounterparty().getInvoiceId())
                        .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBudgets(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckBillingAccountEligibilityRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setHolderName(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getHolderName();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(request.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckBillingAccountEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkBillingAccountEligibility(outbound1);

        var outbound2 = LintPostingBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBank(PostingBatchBankSettlement.newBuilder()
                                .setIban(switch (reply1.getBillingAccount().getFundingCase()) {
                                    case BANK -> reply1.getBillingAccount().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPostingBatches(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckCreditNoteEligibility", request.getCreditNote().getCreditNoteId());
        return CheckCreditNoteEligibilityResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setNumber(reply2.getPostingBatch().getNumber())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNumber(request.getCreditNote().getNumber())
                        .build())
                .setOutcome(CheckCreditNoteEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes a credit note.
     */
    public NormalizeCreditNoteResponse normalizeCreditNote(NormalizeCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var builder0 = ValidateInvoiceRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ValidateInvoiceRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getContact().getPhone())
                        .build())
                .build());
        builder0.setInvoice(Invoice.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                        .setUpdatedAt(request.getCreditNote().getAudit().getUpdatedAt())
                        .build())
                .setBank(InvoiceBankSettlement.newBuilder()
                        .setIban(request.getCreditNote().getBank().getIban())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoice(outbound0);

        var futures1 = request.getCreditNote().getLinesList().stream()
                .map(element -> PreviewInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoice(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? PreviewInvoiceResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeCreditNote", request.getCreditNote().getCreditNoteId());
        return NormalizeCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setStatus(request.getCreditNote().getStatus())
                        .build())
                .setOutcome(NormalizeCreditNoteResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Explains a credit note.
     */
    public ExplainCreditNoteDecisionResponse explainCreditNoteDecision(ExplainCreditNoteDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCreditNoteDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var futures0 = request.getCreditNote().getAttachmentsList().stream()
                .map(element -> EstimateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .addAttachments(AssetTransferAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAssetTransfer(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? EstimateAssetTransferResponse.getDefaultInstance()
                : results0.getLast();

        List<LintBillingAccountsResponse> results1 = request.getCreditNote().getLinesList().stream()
                .map(element -> LintBillingAccountsRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setAccountHolder(reply0.getAssetTransfer().getBank().getAccountHolder())
                                .build())
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintBillingAccountsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBillingAccounts(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? LintBillingAccountsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CheckBillingAccountEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckBillingAccountEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getBillingAccount().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkBillingAccountEligibility(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCreditNoteDecision", request.getCreditNote().getCreditNoteId());
        return ExplainCreditNoteDecisionResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setTotal(request.getCreditNote().getTotal())
                        .build())
                .setOutcome(ExplainCreditNoteDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a credit note.
     */
    public ScoreCreditNoteResponse scoreCreditNote(ScoreCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        List<ExplainInvoiceDecisionResponse> results0 = request.getCreditNote().getAttachmentsList().stream()
                .map(element -> ExplainInvoiceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addAttachments(InvoiceAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceDecision(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ExplainInvoiceDecisionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SummarizeControlsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeControlsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeControls(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreCreditNote", request.getCreditNote().getCreditNoteId());
        return ScoreCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setTotal(request.getCreditNote().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a credit note.
     */
    public LintCreditNotesResponse lintCreditNotes(LintCreditNotesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintCreditNotes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = JsonCodec.roundTrip(FindBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBudgetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCreditNote().getAudit().getCreatedAt())
                        .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build(), FindBudgetsRequest.class);
        var reply0 = budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findBudgets(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintCreditNotes", request.getCreditNote().getCreditNoteId());
        return LintCreditNotesResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setIssuedAt(request.getCreditNote().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a credit note.
     */
    public GetCreditNoteRulesResponse getCreditNoteRules(GetCreditNoteRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCreditNoteRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "GetCreditNoteRules");

        var outbound0 = CalculateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getCreditNoteId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.LoanCalculationService/CalculateLoan", outbound0);
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoan(command0.payload());

        var outbound1 = RpcSupport.unpack(Any.pack(ExplainInvoiceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainInvoiceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getLoan().getContact().getPhone())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()), ExplainInvoiceDecisionRequest.class);
        var reply1 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceDecision(outbound1);
        return GetCreditNoteRulesResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNumber(reply0.getLoan().getHolder().getNumber())
                        .setTotal(reply1.getInvoice().getTotal())
                        .build())
                .build();
    }
}
