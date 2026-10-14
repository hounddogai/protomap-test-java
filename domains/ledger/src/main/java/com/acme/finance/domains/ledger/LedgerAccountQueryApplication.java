package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.CountLedgerAccountsRequest;
import com.acme.ledger.v1.CountLedgerAccountsResponse;
import com.acme.ledger.v1.ExportLedgerAccountsRequest;
import com.acme.ledger.v1.ExportLedgerAccountsResponse;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.LookupLedgerAccountRequest;
import com.acme.ledger.v1.LookupLedgerAccountResponse;
import com.acme.ledger.v1.PeekLedgerAccountRequest;
import com.acme.ledger.v1.PeekLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.ledger.v1.SummarizeLedgerAccountsRequest;
import com.acme.ledger.v1.SummarizeLedgerAccountsResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about ledger accounts, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class LedgerAccountQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(LedgerAccountQueryApplication.class);

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LedgerRepository repository;

    public LedgerAccountQueryApplication(
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LedgerRepository repository) {
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns a ledger account.
     */
    public LookupLedgerAccountResponse lookupLedgerAccount(LookupLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "LookupLedgerAccount");
        return LookupLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public BrowseLedgerAccountsResponse browseLedgerAccounts(BrowseLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "BrowseLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseLedgerAccounts", request.getPage().getPageToken());
        return BrowseLedgerAccountsResponse.getDefaultInstance();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public FindLedgerAccountsResponse findLedgerAccounts(FindLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "FindLedgerAccounts");

        var outbound0 = JsonCodec.roundTrip(ListPostingBatchSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPostingBatchSubscriptionsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build(), ListPostingBatchSubscriptionsRequest.class);
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPostingBatchSubscriptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindLedgerAccounts", request.getPage().getPageToken());
        return FindLedgerAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts ledger accounts by status.
     */
    public CountLedgerAccountsResponse countLedgerAccounts(CountLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "CountLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountLedgerAccounts", request.getPage().getPageToken());
        return CountLedgerAccountsResponse.getDefaultInstance();
    }

    /**
     * Returns several ledger accounts at once.
     */
    public ResolveLedgerAccountsResponse resolveLedgerAccounts(ResolveLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "ResolveLedgerAccounts");
        return ResolveLedgerAccountsResponse.getDefaultInstance();
    }

    /**
     * Streams every matching ledger account.
     */
    public ExportLedgerAccountsResponse exportLedgerAccounts(ExportLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "ExportLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportLedgerAccounts", request.getFilter().getQuery());
        return ExportLedgerAccountsResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Summarizes a ledger account.
     */
    public SummarizeLedgerAccountsResponse summarizeLedgerAccounts(SummarizeLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "SummarizeLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeLedgerAccounts", request.getLedgerAccountId());
        return SummarizeLedgerAccountsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .build();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public SuggestLedgerAccountsResponse suggestLedgerAccounts(SuggestLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "SuggestLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestLedgerAccounts", request.getPage().getPageToken());
        return SuggestLedgerAccountsResponse.getDefaultInstance();
    }

    /**
     * Returns a ledger account.
     */
    public PeekLedgerAccountResponse peekLedgerAccount(PeekLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "PeekLedgerAccount");

        var outbound0 = JsonCodec.roundTrip(ScoreLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build(), ScoreLedgerAccountRequest.class);
        var reply0 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreLedgerAccount(outbound0);
        return PeekLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
