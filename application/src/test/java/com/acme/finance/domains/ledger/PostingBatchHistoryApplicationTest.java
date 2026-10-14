package com.acme.finance.domains.ledger;

import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.ComparePostingBatchVersionsRequest;
import com.acme.ledger.v1.ComparePostingBatchVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetPostingBatchVersionRequest;
import com.acme.ledger.v1.GetPostingBatchVersionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListPostingBatchVersionsRequest;
import com.acme.ledger.v1.ListPostingBatchVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PrunePostingBatchVersionsRequest;
import com.acme.ledger.v1.PrunePostingBatchVersionsResponse;
import com.acme.ledger.v1.RecordPostingBatchVersionRequest;
import com.acme.ledger.v1.RecordPostingBatchVersionResponse;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PostingBatchHistoryApplicationTest {
    @Test
    void listPostingBatchVersionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new PostingBatchHistoryApplication(
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ListPostingBatchVersionsRequest.getDefaultInstance());
        var response = application.listPostingBatchVersions(request);
        var sent0 = channel.request("acme.ledger.v1.PostingBatchNotificationService/StreamPostingBatchNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
    }
}
