package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.ExplainJournalEntryDecisionRequest;
import com.acme.ledger.v1.ExplainJournalEntryDecisionResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetJournalEntryRulesRequest;
import com.acme.ledger.v1.GetJournalEntryRulesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class JournalEntryValidationApplicationTest {
    @Test
    void normalizeJournalEntryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new JournalEntryValidationApplication(
                ChannelRegistry.fixed(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(NormalizeJournalEntryRequest.getDefaultInstance());
        var response = application.normalizeJournalEntry(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "ledger_account", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry", "card", "token"),
                ProbeSupport.value(sent0, "ledger_account", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry_id"),
                ProbeSupport.value(response, "journal_entry", "journal_entry_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "journal_entry", "metadata"),
                ProbeSupport.value(response, "journal_entry", "metadata"));
    }
}
