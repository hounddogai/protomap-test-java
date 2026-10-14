package com.acme.finance.domains.ledger;

import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.CountLedgerAccountsRequest;
import com.acme.ledger.v1.CountLedgerAccountsResponse;
import com.acme.ledger.v1.ExportLedgerAccountsRequest;
import com.acme.ledger.v1.ExportLedgerAccountsResponse;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.LookupLedgerAccountRequest;
import com.acme.ledger.v1.LookupLedgerAccountResponse;
import com.acme.ledger.v1.PeekLedgerAccountRequest;
import com.acme.ledger.v1.PeekLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.ledger.v1.SummarizeLedgerAccountsRequest;
import com.acme.ledger.v1.SummarizeLedgerAccountsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class LedgerAccountQueryApplicationTest {
    @Test
    void exportLedgerAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new LedgerAccountQueryApplication(
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportLedgerAccountsRequest.getDefaultInstance());
        var response = application.exportLedgerAccounts(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "ledger_account", "labels"));
        var endpoint = new LedgerAccountQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportLedgerAccountsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportLedgerAccountsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportLedgerAccounts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void peekLedgerAccountPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new LedgerAccountQueryApplication(
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(PeekLedgerAccountRequest.getDefaultInstance());
        var response = application.peekLedgerAccount(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ledger_account_id"),
                ProbeSupport.value(sent0, "ledger_account", "ledger_account_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ledger_account_id"),
                ProbeSupport.value(response, "ledger_account", "ledger_account_id"));
    }
}
