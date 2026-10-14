package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesRequest;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.ledger.v1.PostingBatch;
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
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FiscalPeriodNotificationApplicationTest {
    @Test
    void streamFiscalPeriodNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new FiscalPeriodNotificationApplication(
                FiscalPeriodAdminServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamFiscalPeriodNoticesRequest.getDefaultInstance());
        var response = application.streamFiscalPeriodNotices(request);
        var sent0 = channel.request("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "fiscal_period", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "fiscal_period", "organizer", "email"));
        var endpoint = new FiscalPeriodNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamFiscalPeriodNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamFiscalPeriodNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamFiscalPeriodNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
