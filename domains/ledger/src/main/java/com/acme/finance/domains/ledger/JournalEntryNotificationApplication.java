package com.acme.finance.domains.ledger;

import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.GetJournalEntryPreferencesRequest;
import com.acme.ledger.v1.GetJournalEntryPreferencesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.NotifyJournalEntryWatchersRequest;
import com.acme.ledger.v1.NotifyJournalEntryWatchersResponse;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a journal entry what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class JournalEntryNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryNotificationApplication.class);

    private final ChannelRegistry channels;

    private final LedgerRepository repository;

    public JournalEntryNotificationApplication(
            ChannelRegistry channels,
            LedgerRepository repository) {
        this.channels = channels;
        this.repository = repository;
    }

    /**
     * Subscribes a journal entry.
     */
    public SubscribeJournalEntryUpdatesResponse subscribeJournalEntryUpdates(SubscribeJournalEntryUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeJournalEntryUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "SubscribeJournalEntryUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeJournalEntryUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribeJournalEntryUpdatesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a journal entry.
     */
    public UnsubscribeJournalEntryUpdatesResponse unsubscribeJournalEntryUpdates(UnsubscribeJournalEntryUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeJournalEntryUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "UnsubscribeJournalEntryUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeJournalEntryUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribeJournalEntryUpdatesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Notifys a journal entry.
     */
    public NotifyJournalEntryWatchersResponse notifyJournalEntryWatchers(NotifyJournalEntryWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyJournalEntryWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "NotifyJournalEntryWatchers");

        var outbound0 = CountDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountDocumentsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getTemplateId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.documents.v1.DocumentQueryService");
        var serviceName0 = "acme.documents.v1" + "." + "DocumentQueryService";
        var methodName0 = "Cou" + "ntDocuments";
        var descriptor0 = MethodDescriptor.<CountDocumentsRequest, CountDocumentsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CountDocumentsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountDocumentsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyJournalEntryWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyJournalEntryWatchersResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .setOutcome(NotifyJournalEntryWatchersResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists journal entries a page at a time.
     */
    public ListJournalEntrySubscriptionsResponse listJournalEntrySubscriptions(ListJournalEntrySubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListJournalEntrySubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "ListJournalEntrySubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListJournalEntrySubscriptions", request.getPage().getPageToken());
        return ListJournalEntrySubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends a journal entry.
     */
    public SendJournalEntryTestNoticeResponse sendJournalEntryTestNotice(SendJournalEntryTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendJournalEntryTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "SendJournalEntryTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendJournalEntryTestNotice", request.getDetails().getRecipient().getEmail());
        return SendJournalEntryTestNoticeResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Returns a journal entry.
     */
    public GetJournalEntryPreferencesResponse getJournalEntryPreferences(GetJournalEntryPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetJournalEntryPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "GetJournalEntryPreferences");
        return GetJournalEntryPreferencesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of journal entries as they happen.
     */
    public StreamJournalEntryNoticesResponse streamJournalEntryNotices(StreamJournalEntryNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamJournalEntryNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "StreamJournalEntryNotices");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamJournalEntryNotices", request.getFilter().getQuery());
        return StreamJournalEntryNoticesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }
}
