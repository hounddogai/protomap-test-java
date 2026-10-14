package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.PostingBatch;

/**
 * The domain's durable record and audit port.
 */
public interface LedgerRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveFiscalPeriod(String tenant, FiscalPeriod value);

    void saveJournalEntry(String tenant, JournalEntry value);

    void saveLedgerAccount(String tenant, LedgerAccount value);

    void savePostingBatch(String tenant, PostingBatch value);
}
