package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.PaymentTerm;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the billing domain. Tenant keys scope every write.
 */
@Repository
public class JdbcBillingRepository implements BillingRepository {
    private final JdbcTemplate jdbc;

    private final BillingAccountRecordMapper billingAccountMapper;

    private final CreditNoteRecordMapper creditNoteMapper;

    private final DunningRecordMapper dunningMapper;

    private final InvoiceRecordMapper invoiceMapper;

    private final PaymentTermRecordMapper paymentTermMapper;

    public JdbcBillingRepository(
            JdbcTemplate jdbc,
            BillingAccountRecordMapper billingAccountMapper,
            CreditNoteRecordMapper creditNoteMapper,
            DunningRecordMapper dunningMapper,
            InvoiceRecordMapper invoiceMapper,
            PaymentTermRecordMapper paymentTermMapper) {
        this.jdbc = jdbc;
        this.billingAccountMapper = billingAccountMapper;
        this.creditNoteMapper = creditNoteMapper;
        this.dunningMapper = dunningMapper;
        this.invoiceMapper = invoiceMapper;
        this.paymentTermMapper = paymentTermMapper;
    }

    private void save(String tenant, String kind, String id, String label, byte[] snapshot) {
        if (id.isBlank()) {
            throw new IllegalArgumentException("record identifier is required");
        }
        int updated = jdbc.update("UPDATE finance_records SET label = ?, snapshot = ? "
                + "WHERE tenant_id = ? AND entity_kind = ? AND record_id = ?", label, snapshot, tenant, kind, id);
        if (updated == 0) {
            try {
                jdbc.update("INSERT INTO finance_records (tenant_id, entity_kind, record_id, label, snapshot) "
                        + "VALUES (?, ?, ?, ?, ?)", tenant, kind, id, label, snapshot);
            } catch (DuplicateKeyException concurrentInsert) {
                jdbc.update("UPDATE finance_records SET label = ?, snapshot = ? "
                        + "WHERE tenant_id = ? AND entity_kind = ? AND record_id = ?", label, snapshot, tenant, kind, id);
            }
        }
    }

    @Override
    public void recordAction(String tenant, String kind, String id, String action) {
        jdbc.update("INSERT INTO finance_actions (tenant_id, entity_kind, record_id, operation) VALUES (?, ?, ?, ?)",
                tenant, kind, id, action);
    }

    @Override
    public void recordDetail(String tenant, String operation, String detail) {
        jdbc.update("INSERT INTO finance_details (tenant_id, operation, detail) VALUES (?, ?, ?)", tenant, operation, detail);
    }

    @Override
    public void saveBillingAccount(String tenant, BillingAccount value) {
        var record = billingAccountMapper.toRecord(value);
        save(tenant, "BillingAccount", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveCreditNote(String tenant, CreditNote value) {
        var record = creditNoteMapper.toRecord(value);
        save(tenant, "CreditNote", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveDunning(String tenant, Dunning value) {
        var record = dunningMapper.toRecord(value);
        save(tenant, "Dunning", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveInvoice(String tenant, Invoice value) {
        var record = invoiceMapper.toRecord(value);
        save(tenant, "Invoice", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePaymentTerm(String tenant, PaymentTerm value) {
        var record = paymentTermMapper.toRecord(value);
        save(tenant, "PaymentTerm", record.id(), record.label(), record.snapshot());
    }
}
