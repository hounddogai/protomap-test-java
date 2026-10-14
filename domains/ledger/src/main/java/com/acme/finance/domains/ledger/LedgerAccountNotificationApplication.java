package com.acme.finance.domains.ledger;

import com.acme.common.v1.ContactInfo;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.GetLedgerAccountPreferencesRequest;
import com.acme.ledger.v1.GetLedgerAccountPreferencesResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a ledger account what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class LedgerAccountNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(LedgerAccountNotificationApplication.class);

    private final LedgerRepository repository;

    public LedgerAccountNotificationApplication(
            LedgerRepository repository) {
        this.repository = repository;
    }

    /**
     * Subscribes a ledger account.
     */
    public SubscribeLedgerAccountUpdatesResponse subscribeLedgerAccountUpdates(SubscribeLedgerAccountUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeLedgerAccountUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "SubscribeLedgerAccountUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeLedgerAccountUpdates", request.getDetails().getRecipient().getPhone());
        return SubscribeLedgerAccountUpdatesResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a ledger account.
     */
    public UnsubscribeLedgerAccountUpdatesResponse unsubscribeLedgerAccountUpdates(UnsubscribeLedgerAccountUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeLedgerAccountUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "UnsubscribeLedgerAccountUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeLedgerAccountUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribeLedgerAccountUpdatesResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Notifys a ledger account.
     */
    public NotifyLedgerAccountWatchersResponse notifyLedgerAccountWatchers(NotifyLedgerAccountWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyLedgerAccountWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "NotifyLedgerAccountWatchers");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyLedgerAccountWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyLedgerAccountWatchersResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    public ListLedgerAccountSubscriptionsResponse listLedgerAccountSubscriptions(ListLedgerAccountSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListLedgerAccountSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "ListLedgerAccountSubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListLedgerAccountSubscriptions", request.getPage().getPageToken());
        return ListLedgerAccountSubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends a ledger account.
     */
    public SendLedgerAccountTestNoticeResponse sendLedgerAccountTestNotice(SendLedgerAccountTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendLedgerAccountTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "SendLedgerAccountTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendLedgerAccountTestNotice", request.getDetails().getRecipient().getPhone());
        return SendLedgerAccountTestNoticeResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Returns a ledger account.
     */
    public GetLedgerAccountPreferencesResponse getLedgerAccountPreferences(GetLedgerAccountPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLedgerAccountPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "GetLedgerAccountPreferences");
        return GetLedgerAccountPreferencesResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of ledger accounts as they happen.
     */
    public StreamLedgerAccountNoticesResponse streamLedgerAccountNotices(StreamLedgerAccountNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamLedgerAccountNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getContext().getTraceId(), "StreamLedgerAccountNotices");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamLedgerAccountNotices", request.getFilter().getQuery());
        return StreamLedgerAccountNoticesResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .build();
    }
}
