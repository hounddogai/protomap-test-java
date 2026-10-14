package com.acme.finance.domains.ledger;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.GetFiscalPeriodVersionRequest;
import com.acme.ledger.v1.GetFiscalPeriodVersionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FiscalPeriodHistoryApplicationTest {
    @Test
    void revertFiscalPeriodPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new FiscalPeriodHistoryApplication(
                repository);
        var request = ProbeSupport.sample(RevertFiscalPeriodRequest.getDefaultInstance());
        var response = application.revertFiscalPeriod(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "fiscal_period_id"),
                ProbeSupport.value(response, "fiscal_period", "fiscal_period_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fiscal_period", "title"),
                ProbeSupport.value(response, "fiscal_period", "title"));
    }
}
