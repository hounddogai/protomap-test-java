package com.acme.finance.domains.billing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionMeasurement;
import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.admissions.v1.CreateAdmissionRequest;
import com.acme.admissions.v1.CreateAdmissionResponse;
import com.acme.admissions.v1.GateScanRef;
import com.acme.admissions.v1.ListAdmissionsRequest;
import com.acme.admissions.v1.ListAdmissionsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.WatchBankAccountsRequest;
import com.acme.banking.v1.WatchBankAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CalculateCreditNoteRequest;
import com.acme.billing.v1.CalculateCreditNoteResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.ExplainBillingAccountDecisionRequest;
import com.acme.billing.v1.ExplainBillingAccountDecisionResponse;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.ValidateBillingAccountRequest;
import com.acme.billing.v1.ValidateBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.SummarizeLedgerAccountsRequest;
import com.acme.ledger.v1.SummarizeLedgerAccountsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.PayRunServiceGrpc;
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
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a credit note, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CreditNoteCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(CreditNoteCalculationApplication.class);

    private final AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking;

    private final AdmissionServiceGrpc.AdmissionServiceFutureStub admissionsAdmissionServiceFuture;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceFutureStub payrollPayRunServiceFuture;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public CreditNoteCalculationApplication(
            AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking,
            AdmissionServiceGrpc.AdmissionServiceFutureStub admissionsAdmissionServiceFuture,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            PayRunServiceGrpc.PayRunServiceFutureStub payrollPayRunServiceFuture,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.admissionsAdmissionServiceBlocking = admissionsAdmissionServiceBlocking;
        this.admissionsAdmissionServiceFuture = admissionsAdmissionServiceFuture;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.bankingBankAccountServiceBlocking = bankingBankAccountServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.payrollPayRunServiceFuture = payrollPayRunServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a credit note.
     */
    public CalculateCreditNoteResponse calculateCreditNote(CalculateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ValidateBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCreditNote().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setInvoiceId(request.getCreditNote().getCounterparty().getInvoiceId())
                                .setNumber(request.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBillingAccount(outbound0), rpcExecutor);

        var outbound1 = ListLedgerAccountSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountSubscriptionsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccountSubscriptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateCreditNote", request.getCreditNote().getCreditNoteId());
        return CalculateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNumber(request.getCreditNote().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a credit note.
     */
    public EstimateCreditNoteResponse estimateCreditNote(EstimateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = NormalizeBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBillingAccount(outbound0);

        var outbound1 = SummarizeLedgerAccountsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeLedgerAccountsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getBillingAccount().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeLedgerAccounts(outbound1);

        var outbound2 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getBillingAccount().getLabelsMap())
                        .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(outbound2));
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateCreditNote", request.getCreditNote().getCreditNoteId());
        return EstimateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getBillingAccount().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setInvoiceId(reply0.getBillingAccount().getHolder().getInvoiceId())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNotes(request.getCreditNote().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a credit note.
     */
    public QuoteCreditNoteResponse quoteCreditNote(QuoteCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var results0 = new ArrayList<ListAdmissionsResponse>();
        for (var element : request.getCreditNote().getLinesList()) {
            ListAdmissionsRequest itemRequest = ListAdmissionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAdmissionsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
            results0.add(admissionsAdmissionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAdmissions(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ListAdmissionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ScoreBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setIban(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBillingAccount(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteCreditNote", request.getCreditNote().getCreditNoteId());
        return QuoteCreditNoteResponse.newBuilder()
                .setBreakdown(QuoteCreditNoteResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setStatus(request.getCreditNote().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a credit note.
     */
    public CompareCreditNoteOptionsResponse compareCreditNoteOptions(CompareCreditNoteOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareCreditNoteOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = BatchGetPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = payrollPayRunServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetPayRuns(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setIban(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(outbound1);

        var outbound2 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setHolder(PostingBatchRef.newBuilder()
                                .setPostingBatchId(reply1.getLedgerAccount().getHolder().getPostingBatchId())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(value));

        var outbound3 = CreateAdmissionRequest.newBuilder()
                .setAdmission(Admission.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AdmissionMeasurement.newBuilder()
                                .setObservedAt(reply1.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .setOccurredAt(request.getCreditNote().getIssuedAt())
                        .setSubject(GateScanRef.newBuilder()
                                .setDisplayName(reply1.getLedgerAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending3 = admissionsAdmissionServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).createAdmission(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareCreditNoteOptions", request.getCreditNote().getCreditNoteId());
        return CompareCreditNoteOptionsResponse.newBuilder()
                .setBreakdown(CompareCreditNoteOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLedgerAccount().getAudit().getCreatedAt())
                                .setUpdatedBy(reply2.getLedgerAccount().getAudit().getUpdatedBy())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .addLines(CreditNoteLine.newBuilder()
                                .putAllAttributes(reply3.getAdmission().getAttributesMap())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates a credit note.
     */
    public RecalculateCreditNotesResponse recalculateCreditNotes(RecalculateCreditNotesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateCreditNotes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = NormalizeBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setNetwork(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBillingAccount(outbound0);

        var outbound1 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamLedgerAccountNoticesRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getBillingAccount().getLabelsMap())
                        .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(reply0.getBillingAccount().getContact().getPhone())
                        .setPreferredChannel(reply0.getBillingAccount().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound1);
        var reply1 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateCreditNotes", request.getCreditNote().getCreditNoteId());
        return RecalculateCreditNotesResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(request.getCreditNote().getBillingAddress())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setInvoiceId(reply0.getBillingAccount().getHolder().getInvoiceId())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .build();
    }

    /**
     * Simulates a credit note.
     */
    public SimulateCreditNoteResponse simulateCreditNote(SimulateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ValidateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ValidateAssetTransferRequest, ValidateAssetTransferResponse> port0 = RpcPort.adapt(assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateAssetTransfer);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ValidateBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .setIban(switch (reply0.getAssetTransfer().getSettlementCase()) {
                                    case BANK -> reply0.getAssetTransfer().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .setNumber(request.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBillingAccount(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateCreditNote", request.getCreditNote().getCreditNoteId());
        return SimulateCreditNoteResponse.newBuilder()
                .setBreakdown(SimulateCreditNoteResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getAssetTransfer().getTotal())
                        .build())
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(request.getCreditNote().getCounterparty())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a credit note.
     */
    public ExplainCreditNoteCalculationResponse explainCreditNoteCalculation(ExplainCreditNoteCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCreditNoteCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ExplainBillingAccountDecisionRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setIban(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBillingAccountDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBillingAccountDecision(outbound0);

        var outbound1 = LintBillingAccountsRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setAccountHolder(reply0.getBillingAccount().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBillingAccounts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCreditNoteCalculation", request.getCreditNote().getCreditNoteId());
        return ExplainCreditNoteCalculationResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setInvoiceId(reply0.getBillingAccount().getHolder().getInvoiceId())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setTotal(request.getCreditNote().getTotal())
                        .build())
                .setOutcome(ExplainCreditNoteCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a credit note.
     */
    public GetCreditNoteRatesResponse getCreditNoteRates(GetCreditNoteRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCreditNoteRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "GetCreditNoteRates");

        var outbound0 = WatchBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchBankAccountsRequest.Filter.newBuilder()
                        .setOwnerId(request.getCreditNoteId())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchBankAccounts(outbound0);
        var reply0 = WatchBankAccountsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        return GetCreditNoteRatesResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .build();
    }
}
