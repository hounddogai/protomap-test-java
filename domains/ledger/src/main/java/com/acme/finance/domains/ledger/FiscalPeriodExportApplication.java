package com.acme.finance.domains.ledger;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CancelFiscalPeriodExportRequest;
import com.acme.ledger.v1.CancelFiscalPeriodExportResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetFiscalPeriodExportRequest;
import com.acme.ledger.v1.GetFiscalPeriodExportResponse;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Exports fiscal periods to files and other systems. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class FiscalPeriodExportApplication {
    private static final Logger log = LoggerFactory.getLogger(FiscalPeriodExportApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final LedgerRepository repository;

    public FiscalPeriodExportApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            LedgerRepository repository) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.repository = repository;
    }

    /**
     * Streams every matching fiscal period.
     */
    public StreamFiscalPeriodExportResponse streamFiscalPeriodExport(StreamFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getContext().getTraceId(), "StreamFiscalPeriodExport");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFiscalPeriodExport", request.getFilter().getQuery());
        return StreamFiscalPeriodExportResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a fiscal period.
     */
    public GetFiscalPeriodExportResponse getFiscalPeriodExport(GetFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "GetFiscalPeriodExport");
        return GetFiscalPeriodExportResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .build();
    }

    /**
     * Cancels a fiscal period.
     */
    public CancelFiscalPeriodExportResponse cancelFiscalPeriodExport(CancelFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "CancelFiscalPeriodExport");

        var outbound0 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound0);
        var reply0 = WatchDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelFiscalPeriodExport", request.getDetails().getReasonCode());
        return CancelFiscalPeriodExportResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(CancelFiscalPeriodExportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Schedules a fiscal period.
     */
    public ScheduleFiscalPeriodExportResponse scheduleFiscalPeriodExport(ScheduleFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "ScheduleFiscalPeriodExport");
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleFiscalPeriodExport", request.getFiscalPeriodId());
        return ScheduleFiscalPeriodExportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .build();
    }

    /**
     * Retrys a fiscal period.
     */
    public RetryFiscalPeriodExportResponse retryFiscalPeriodExport(RetryFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFiscalPeriod(request.getContext().getTenantId(), request.getFiscalPeriod());
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriod().getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryFiscalPeriodExport", request.getFiscalPeriod().getFiscalPeriodId());
        return RetryFiscalPeriodExportResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setWindow(request.getFiscalPeriod().getWindow())
                        .build())
                .build();
    }

    /**
     * Shares a fiscal period.
     */
    public ShareFiscalPeriodExportResponse shareFiscalPeriodExport(ShareFiscalPeriodExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareFiscalPeriodExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "ShareFiscalPeriodExport");

        var outbound0 = UpdateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setBlock(AccessPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAccessPolicy(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareFiscalPeriodExport", request.getDetails().getRecipient().getPhone());
        return ShareFiscalPeriodExportResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
