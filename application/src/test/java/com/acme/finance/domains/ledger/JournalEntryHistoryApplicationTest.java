package com.acme.finance.domains.ledger;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PruneJournalEntryVersionsRequest;
import com.acme.ledger.v1.PruneJournalEntryVersionsResponse;
import com.acme.ledger.v1.RecordJournalEntryVersionRequest;
import com.acme.ledger.v1.RecordJournalEntryVersionResponse;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class JournalEntryHistoryApplicationTest {
    @Test
    void revertJournalEntryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new JournalEntryHistoryApplication(
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(RevertJournalEntryRequest.getDefaultInstance());
        var response = application.revertJournalEntry(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry_id"),
                ProbeSupport.value(response, "journal_entry", "journal_entry_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry", "issued_at"),
                ProbeSupport.value(response, "journal_entry", "issued_at"));
    }
}
