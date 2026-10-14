package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetFiscalPeriodVersionRequest;
import com.acme.ledger.v1.GetFiscalPeriodVersionResponse;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of a fiscal period and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class FiscalPeriodHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(FiscalPeriodHistoryApplication.class);

    private final LedgerRepository repository;

    public FiscalPeriodHistoryApplication(
            LedgerRepository repository) {
        this.repository = repository;
    }

    /**
     * Records a fiscal period.
     */
    public RecordFiscalPeriodVersionResponse recordFiscalPeriodVersion(RecordFiscalPeriodVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordFiscalPeriodVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFiscalPeriod(request.getContext().getTenantId(), request.getFiscalPeriod());
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriod().getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordFiscalPeriodVersion", request.getFiscalPeriod().getFiscalPeriodId());
        return RecordFiscalPeriodVersionResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(request.getFiscalPeriod().getOrganizer())
                        .build())
                .build();
    }

    /**
     * Lists fiscal periods a page at a time.
     */
    public ListFiscalPeriodVersionsResponse listFiscalPeriodVersions(ListFiscalPeriodVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFiscalPeriodVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getContext().getTraceId(), "ListFiscalPeriodVersions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFiscalPeriodVersions", request.getPage().getPageToken());
        return ListFiscalPeriodVersionsResponse.getDefaultInstance();
    }

    /**
     * Returns a fiscal period.
     */
    public GetFiscalPeriodVersionResponse getFiscalPeriodVersion(GetFiscalPeriodVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFiscalPeriodVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "GetFiscalPeriodVersion");
        return GetFiscalPeriodVersionResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .build();
    }

    /**
     * Compares a fiscal period.
     */
    public CompareFiscalPeriodVersionsResponse compareFiscalPeriodVersions(CompareFiscalPeriodVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareFiscalPeriodVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFiscalPeriod(request.getContext().getTenantId(), request.getFiscalPeriod());
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriod().getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareFiscalPeriodVersions", request.getFiscalPeriod().getFiscalPeriodId());
        return CompareFiscalPeriodVersionsResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setTitle(request.getFiscalPeriod().getTitle())
                        .build())
                .build();
    }

    /**
     * Reverts a fiscal period.
     */
    public RevertFiscalPeriodResponse revertFiscalPeriod(RevertFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFiscalPeriod(request.getContext().getTenantId(), request.getFiscalPeriod());
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriod().getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertFiscalPeriod", request.getFiscalPeriod().getFiscalPeriodId());
        return RevertFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setTitle(request.getFiscalPeriod().getTitle())
                        .build())
                .build();
    }

    /**
     * Deletes a fiscal period.
     */
    public PruneFiscalPeriodVersionsResponse pruneFiscalPeriodVersions(PruneFiscalPeriodVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneFiscalPeriodVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "PruneFiscalPeriodVersions");
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneFiscalPeriodVersions", request.getFiscalPeriodId());
        return PruneFiscalPeriodVersionsResponse.getDefaultInstance();
    }
}
