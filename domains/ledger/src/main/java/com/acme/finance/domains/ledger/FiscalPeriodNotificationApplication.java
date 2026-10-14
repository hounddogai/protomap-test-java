package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesRequest;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesResponse;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.ReassignFiscalPeriodRequest;
import com.acme.ledger.v1.ReassignFiscalPeriodResponse;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a fiscal period what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FiscalPeriodNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(FiscalPeriodNotificationApplication.class);

    private final FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LedgerRepository repository;

    public FiscalPeriodNotificationApplication(
            FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LedgerRepository repository) {
        this.ledgerFiscalPeriodAdminServiceBlocking = ledgerFiscalPeriodAdminServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Subscribes a fiscal period.
     */
    public SubscribeFiscalPeriodUpdatesResponse subscribeFiscalPeriodUpdates(SubscribeFiscalPeriodUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeFiscalPeriodUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "SubscribeFiscalPeriodUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeFiscalPeriodUpdates", request.getDetails().getRecipient().getPhone());
        return SubscribeFiscalPeriodUpdatesResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a fiscal period.
     */
    public UnsubscribeFiscalPeriodUpdatesResponse unsubscribeFiscalPeriodUpdates(UnsubscribeFiscalPeriodUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeFiscalPeriodUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "UnsubscribeFiscalPeriodUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeFiscalPeriodUpdates", request.getDetails().getRecipient().getPhone());
        return UnsubscribeFiscalPeriodUpdatesResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Notifys a fiscal period.
     */
    public NotifyFiscalPeriodWatchersResponse notifyFiscalPeriodWatchers(NotifyFiscalPeriodWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyFiscalPeriodWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "NotifyFiscalPeriodWatchers");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyFiscalPeriodWatchers", request.getDetails().getRecipient().getPhone());
        return NotifyFiscalPeriodWatchersResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists fiscal periods a page at a time.
     */
    public ListFiscalPeriodSubscriptionsResponse listFiscalPeriodSubscriptions(ListFiscalPeriodSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFiscalPeriodSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getContext().getTraceId(), "ListFiscalPeriodSubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFiscalPeriodSubscriptions", request.getPage().getPageToken());
        return ListFiscalPeriodSubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends a fiscal period.
     */
    public SendFiscalPeriodTestNoticeResponse sendFiscalPeriodTestNotice(SendFiscalPeriodTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendFiscalPeriodTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "SendFiscalPeriodTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendFiscalPeriodTestNotice", request.getDetails().getRecipient().getPhone());
        return SendFiscalPeriodTestNoticeResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a fiscal period.
     */
    public GetFiscalPeriodPreferencesResponse getFiscalPeriodPreferences(GetFiscalPeriodPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFiscalPeriodPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "GetFiscalPeriodPreferences");

        var outbound0 = JsonCodec.roundTrip(ReassignFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReassignFiscalPeriodRequest.Details.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setFiscalPeriodId(request.getFiscalPeriodId())
                .build(), ReassignFiscalPeriodRequest.class);
        var reply0 = ledgerFiscalPeriodAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reassignFiscalPeriod(outbound0);
        return GetFiscalPeriodPreferencesResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of fiscal periods as they happen.
     */
    public StreamFiscalPeriodNoticesResponse streamFiscalPeriodNotices(StreamFiscalPeriodNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFiscalPeriodNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getContext().getTraceId(), "StreamFiscalPeriodNotices");

        var outbound0 = ListPostingBatchSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPostingBatchSubscriptionsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPostingBatchSubscriptions(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFiscalPeriodNotices", request.getFilter().getQuery());
        return StreamFiscalPeriodNoticesResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
