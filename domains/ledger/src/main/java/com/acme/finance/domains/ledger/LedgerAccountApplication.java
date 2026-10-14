package com.acme.finance.domains.ledger;

import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.GetLedgerAccountRequest;
import com.acme.ledger.v1.GetLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Manages ledger accounts: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class LedgerAccountApplication {
    private static final Logger log = LoggerFactory.getLogger(LedgerAccountApplication.class);

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final LedgerRepository repository;

    public LedgerAccountApplication(
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            LedgerRepository repository) {
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.repository = repository;
    }

    /**
     * Creates a ledger account.
     */
    public CreateLedgerAccountResponse createLedgerAccount(CreateLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return CreateLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(request.getLedgerAccount().getContact())
                        .setOpenedAt(request.getLedgerAccount().getOpenedAt())
                        .build())
                .build();
    }

    /**
     * Returns a ledger account.
     */
    public GetLedgerAccountResponse getLedgerAccount(GetLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "GetLedgerAccount");
        return GetLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public ListLedgerAccountsResponse listLedgerAccounts(ListLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "ListLedgerAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListLedgerAccounts", request.getPage().getPageToken());
        return ListLedgerAccountsResponse.getDefaultInstance();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public SearchLedgerAccountsResponse searchLedgerAccounts(SearchLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "SearchLedgerAccounts");

        var outbound0 = RpcSupport.unpack(Any.pack(UnsubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(UnsubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build()), UnsubscribeFiscalPeriodUpdatesRequest.class);
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeFiscalPeriodUpdates(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchLedgerAccounts", request.getPage().getPageToken());
        return SearchLedgerAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of ledger accounts as they happen.
     */
    public WatchLedgerAccountsResponse watchLedgerAccounts(WatchLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "WatchLedgerAccounts");

        var outbound0 = ResolveLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLedgerAccounts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchLedgerAccounts", request.getFilter().getQuery());
        return WatchLedgerAccountsResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Creates a ledger account.
     */
    public CloneLedgerAccountResponse cloneLedgerAccount(CloneLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());

        var outbound0 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLedgerAccount().getAudit().getUpdatedAt())
                        .putAllLabels(request.getLedgerAccount().getLabelsMap())
                        .setOwnerId(request.getLedgerAccount().getLedgerAccountId())
                        .build())
                .build();
        var stream0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound0);
        var reply0 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getHolder().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneLedgerAccount", request.getLedgerAccount().getHolder().getPostingBatchId());
        return CloneLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setBalance(request.getLedgerAccount().getBalance())
                        .setCurrencyCode(request.getLedgerAccount().getCurrencyCode())
                        .build())
                .build();
    }
}
