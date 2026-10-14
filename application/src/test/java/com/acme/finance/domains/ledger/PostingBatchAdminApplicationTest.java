package com.acme.finance.domains.ledger;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.ArchivePostingBatchRequest;
import com.acme.ledger.v1.ArchivePostingBatchResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.InspectPostingBatchRequest;
import com.acme.ledger.v1.InspectPostingBatchResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LockPostingBatchRequest;
import com.acme.ledger.v1.LockPostingBatchResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PurgePostingBatchRequest;
import com.acme.ledger.v1.PurgePostingBatchResponse;
import com.acme.ledger.v1.ReassignPostingBatchRequest;
import com.acme.ledger.v1.ReassignPostingBatchResponse;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.ledger.v1.RestorePostingBatchRequest;
import com.acme.ledger.v1.RestorePostingBatchResponse;
import com.acme.ledger.v1.TransferPostingBatchRequest;
import com.acme.ledger.v1.TransferPostingBatchResponse;
import com.acme.ledger.v1.UnlockPostingBatchRequest;
import com.acme.ledger.v1.UnlockPostingBatchResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PostingBatchAdminApplicationTest {
    @Test
    void unlockPostingBatchPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new PostingBatchAdminApplication(
                repository);
        var request = ProbeSupport.sample(UnlockPostingBatchRequest.getDefaultInstance());
        var response = application.unlockPostingBatch(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "posting_batch_id"),
                ProbeSupport.value(response, "posting_batch", "posting_batch_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
