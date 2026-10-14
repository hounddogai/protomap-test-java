package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.GetPostingBatchPreferencesRequest;
import com.acme.ledger.v1.GetPostingBatchPreferencesResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesResponse;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a posting batch what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PostingBatchNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(PostingBatchNotificationApplication.class);

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LedgerRepository repository;

    public PostingBatchNotificationApplication(
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LedgerRepository repository) {
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.repository = repository;
    }

    /**
     * Subscribes a posting batch.
     */
    public SubscribePostingBatchUpdatesResponse subscribePostingBatchUpdates(SubscribePostingBatchUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribePostingBatchUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "SubscribePostingBatchUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribePostingBatchUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribePostingBatchUpdatesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a posting batch.
     */
    public UnsubscribePostingBatchUpdatesResponse unsubscribePostingBatchUpdates(UnsubscribePostingBatchUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribePostingBatchUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "UnsubscribePostingBatchUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribePostingBatchUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribePostingBatchUpdatesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Notifys a posting batch.
     */
    public NotifyPostingBatchWatchersResponse notifyPostingBatchWatchers(NotifyPostingBatchWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyPostingBatchWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "NotifyPostingBatchWatchers");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyPostingBatchWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyPostingBatchWatchersResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Lists posting batches a page at a time.
     */
    public ListPostingBatchSubscriptionsResponse listPostingBatchSubscriptions(ListPostingBatchSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPostingBatchSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getContext().getTraceId(), "ListPostingBatchSubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPostingBatchSubscriptions", request.getPage().getPageToken());
        return ListPostingBatchSubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends a posting batch.
     */
    public SendPostingBatchTestNoticeResponse sendPostingBatchTestNotice(SendPostingBatchTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendPostingBatchTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "SendPostingBatchTestNotice");

        var outbound0 = RetryFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryFiscalPeriodExportRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getRecipient().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<RetryFiscalPeriodExportRequest, RetryFiscalPeriodExportResponse>() {
            @Override
            public RetryFiscalPeriodExportResponse apply(RetryFiscalPeriodExportRequest input) {
                return ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryFiscalPeriodExport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendPostingBatchTestNotice", request.getDetails().getRecipient().getEmail());
        return SendPostingBatchTestNoticeResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a posting batch.
     */
    public GetPostingBatchPreferencesResponse getPostingBatchPreferences(GetPostingBatchPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPostingBatchPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "GetPostingBatchPreferences");
        return GetPostingBatchPreferencesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of posting batches as they happen.
     */
    public StreamPostingBatchNoticesResponse streamPostingBatchNotices(StreamPostingBatchNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamPostingBatchNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getContext().getTraceId(), "StreamPostingBatchNotices");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamPostingBatchNotices", request.getFilter().getQuery());
        return StreamPostingBatchNoticesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }
}
