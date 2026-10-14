package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.ConcessionStand;
import com.acme.concessions.v1.ConcessionStandCalculationServiceGrpc;
import com.acme.concessions.v1.RecalculateConcessionStandsRequest;
import com.acme.concessions.v1.RecalculateConcessionStandsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.expenses.v1.BrowseReceiptsRequest;
import com.acme.expenses.v1.BrowseReceiptsResponse;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CountReceiptsRequest;
import com.acme.expenses.v1.CountReceiptsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExportReceiptsRequest;
import com.acme.expenses.v1.ExportReceiptsResponse;
import com.acme.expenses.v1.FindReceiptsRequest;
import com.acme.expenses.v1.FindReceiptsResponse;
import com.acme.expenses.v1.LookupReceiptRequest;
import com.acme.expenses.v1.LookupReceiptResponse;
import com.acme.expenses.v1.PeekReceiptRequest;
import com.acme.expenses.v1.PeekReceiptResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SuggestReceiptsRequest;
import com.acme.expenses.v1.SuggestReceiptsResponse;
import com.acme.expenses.v1.SummarizeReceiptsRequest;
import com.acme.expenses.v1.SummarizeReceiptsResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.RecordPostingBatchVersionRequest;
import com.acme.ledger.v1.RecordPostingBatchVersionResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about receipts, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ReceiptQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptQueryApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceFutureStub auditAuditTrailValidationServiceFuture;

    private final ConcessionStandCalculationServiceGrpc.ConcessionStandCalculationServiceBlockingStub concessionsConcessionStandCalculationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking;

    private final ExpensesRepository repository;

    public ReceiptQueryApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceFutureStub auditAuditTrailValidationServiceFuture,
            ConcessionStandCalculationServiceGrpc.ConcessionStandCalculationServiceBlockingStub concessionsConcessionStandCalculationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking,
            ExpensesRepository repository) {
        this.auditAuditTrailValidationServiceFuture = auditAuditTrailValidationServiceFuture;
        this.concessionsConcessionStandCalculationServiceBlocking = concessionsConcessionStandCalculationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.events = events;
        this.expensesExpensePolicyCalculationServiceBlocking = expensesExpensePolicyCalculationServiceBlocking;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.ledgerPostingBatchHistoryServiceBlocking = ledgerPostingBatchHistoryServiceBlocking;
        this.repository = repository;
    }

    public record FindReceiptsStage0Requested(RecalculateExpenseReportsRequest request, CompletableFuture<RecalculateExpenseReportsResponse> completion, io.grpc.Context context) {}

    public record SummarizeReceiptsStage2Requested(SearchVenuesRequest request, CompletableFuture<SearchVenuesResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a receipt.
     */
    public LookupReceiptResponse lookupReceipt(LookupReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "LookupReceipt");
        return LookupReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Lists receipts a page at a time.
     */
    public BrowseReceiptsResponse browseReceipts(BrowseReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "BrowseReceipts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseReceipts", request.getPage().getPageToken());
        return BrowseReceiptsResponse.getDefaultInstance();
    }

    /**
     * Lists receipts a page at a time.
     */
    public FindReceiptsResponse findReceipts(FindReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "FindReceipts");

        var outbound0 = RecalculateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<RecalculateExpenseReportsResponse>();
        events.publishEvent(new FindReceiptsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindReceipts", request.getPage().getPageToken());
        return FindReceiptsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts receipts by status.
     */
    public CountReceiptsResponse countReceipts(CountReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "CountReceipts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountReceipts", request.getPage().getPageToken());
        return CountReceiptsResponse.getDefaultInstance();
    }

    /**
     * Streams every matching receipt.
     */
    public ExportReceiptsResponse exportReceipts(ExportReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "ExportReceipts");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateExpensePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), CalculateExpensePolicyRequest.class);
        var reply0 = expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpensePolicy(outbound0);

        var outbound1 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getExpensePolicy().getOwner().getEmail())
                        .build())
                .build();
        var stream1 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound1);
        var reply1 = WatchDocumentsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportReceipts", request.getFilter().getQuery());
        return ExportReceiptsResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Summarizes a receipt.
     */
    public SummarizeReceiptsResponse summarizeReceipts(SummarizeReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "SummarizeReceipts");

        var outbound0 = CheckAuditTrailEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckAuditTrailEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = auditAuditTrailValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkAuditTrailEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ValidateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setBic(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case BANK -> reply0.getAuditTrail().getBank().getBic();
                                    default -> "";
                                })
                                .setIban(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case BANK -> reply0.getAuditTrail().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAuditTrail().getBillingAddress().getCountryCode())
                                .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateExpenseReport(outbound1);

        var outbound2 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setRegion(reply1.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<SearchVenuesResponse>();
        events.publishEvent(new SummarizeReceiptsStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeReceipts", request.getReceiptId());
        return SummarizeReceiptsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists receipts a page at a time.
     */
    public SuggestReceiptsResponse suggestReceipts(SuggestReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "SuggestReceipts");

        var builder0 = RecalculateConcessionStandsRequest.newBuilder();
        builder0.setConcessionStand(ConcessionStand.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(RecalculateConcessionStandsRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = concessionsConcessionStandCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConcessionStands(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestReceipts", request.getPage().getPageToken());
        return SuggestReceiptsResponse.newBuilder()
                .addReceipts(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getConcessionStand().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public PeekReceiptResponse peekReceipt(PeekReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "PeekReceipt");

        var outbound0 = (RecordPostingBatchVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordPostingBatchVersionRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = ledgerPostingBatchHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordPostingBatchVersion(outbound0);
        return PeekReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
