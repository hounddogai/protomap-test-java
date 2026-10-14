package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.GetLedgerAccountVersionRequest;
import com.acme.ledger.v1.GetLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.ledger.v1.PruneLedgerAccountVersionsRequest;
import com.acme.ledger.v1.PruneLedgerAccountVersionsResponse;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of a ledger account and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class LedgerAccountHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(LedgerAccountHistoryApplication.class);

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LedgerRepository repository;

    public LedgerAccountHistoryApplication(
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LedgerRepository repository) {
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.repository = repository;
    }

    /**
     * Records a ledger account.
     */
    public RecordLedgerAccountVersionResponse recordLedgerAccountVersion(RecordLedgerAccountVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordLedgerAccountVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordLedgerAccountVersion", request.getLedgerAccount().getLedgerAccountId());
        return RecordLedgerAccountVersionResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCurrencyCode(request.getLedgerAccount().getCurrencyCode())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public ListLedgerAccountVersionsResponse listLedgerAccountVersions(ListLedgerAccountVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListLedgerAccountVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "ListLedgerAccountVersions");

        var outbound0 = BeanCopies.copy(RetryFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), RetryFiscalPeriodExportRequest.class);
        var reply0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryFiscalPeriodExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListLedgerAccountVersions", request.getPage().getPageToken());
        return ListLedgerAccountVersionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a ledger account.
     */
    public GetLedgerAccountVersionResponse getLedgerAccountVersion(GetLedgerAccountVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLedgerAccountVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "GetLedgerAccountVersion");
        return GetLedgerAccountVersionResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Compares a ledger account.
     */
    public CompareLedgerAccountVersionsResponse compareLedgerAccountVersions(CompareLedgerAccountVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareLedgerAccountVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareLedgerAccountVersions", request.getLedgerAccount().getLedgerAccountId());
        return CompareLedgerAccountVersionsResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAvailable(request.getLedgerAccount().getAvailable())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Reverts a ledger account.
     */
    public RevertLedgerAccountResponse revertLedgerAccount(RevertLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return RevertLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .putAllLabels(request.getLedgerAccount().getLabelsMap())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Annotates a ledger account.
     */
    public AnnotateLedgerAccountVersionResponse annotateLedgerAccountVersion(AnnotateLedgerAccountVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotateLedgerAccountVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotateLedgerAccountVersion", request.getLedgerAccount().getLedgerAccountId());
        return AnnotateLedgerAccountVersionResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .setStatus(request.getLedgerAccount().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a ledger account.
     */
    public PruneLedgerAccountVersionsResponse pruneLedgerAccountVersions(PruneLedgerAccountVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneLedgerAccountVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "PruneLedgerAccountVersions");
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneLedgerAccountVersions", request.getLedgerAccountId());
        return PruneLedgerAccountVersionsResponse.getDefaultInstance();
    }
}
