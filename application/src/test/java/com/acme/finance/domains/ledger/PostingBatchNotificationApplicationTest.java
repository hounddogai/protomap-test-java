package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.GetPostingBatchPreferencesRequest;
import com.acme.ledger.v1.GetPostingBatchPreferencesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
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
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PostingBatchNotificationApplicationTest {
    @Test
    void sendPostingBatchTestNoticePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new PostingBatchNotificationApplication(
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SendPostingBatchTestNoticeRequest.getDefaultInstance());
        var response = application.sendPostingBatchTestNotice(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(sent0, "fiscal_period", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport"), "fiscal_period", "audit", "updated_by"),
                ProbeSupport.value(response, "posting_batch", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "posting_batch_id"),
                ProbeSupport.value(response, "posting_batch", "posting_batch_id"));
    }

    @Test
    void streamPostingBatchNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new PostingBatchNotificationApplication(
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamPostingBatchNoticesRequest.getDefaultInstance());
        var response = application.streamPostingBatchNotices(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "posting_batch", "billing_address", "region"));
        var endpoint = new PostingBatchNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamPostingBatchNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamPostingBatchNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamPostingBatchNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
