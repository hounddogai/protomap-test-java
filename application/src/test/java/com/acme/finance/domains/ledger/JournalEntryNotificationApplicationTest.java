package com.acme.finance.domains.ledger;

import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetJournalEntryPreferencesRequest;
import com.acme.ledger.v1.GetJournalEntryPreferencesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.NotifyJournalEntryWatchersRequest;
import com.acme.ledger.v1.NotifyJournalEntryWatchersResponse;
import com.acme.ledger.v1.PostingBatch;
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
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class JournalEntryNotificationApplicationTest {
    @Test
    void notifyJournalEntryWatchersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new JournalEntryNotificationApplication(
                ChannelRegistry.fixed(channel),
                repository);
        var request = ProbeSupport.sample(NotifyJournalEntryWatchersRequest.getDefaultInstance());
        var response = application.notifyJournalEntryWatchers(request);
        var sent0 = channel.request("acme.documents.v1.DocumentQueryService/CountDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "template_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentQueryService/CountDocuments"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry_id"),
                ProbeSupport.value(response, "journal_entry", "journal_entry_id"));
    }

    @Test
    void streamJournalEntryNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new JournalEntryNotificationApplication(
                ChannelRegistry.fixed(channel),
                repository);
        var request = ProbeSupport.sample(StreamJournalEntryNoticesRequest.getDefaultInstance());
        var response = application.streamJournalEntryNotices(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "journal_entry", "billing_address", "region"));
        var endpoint = new JournalEntryNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamJournalEntryNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamJournalEntryNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamJournalEntryNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
