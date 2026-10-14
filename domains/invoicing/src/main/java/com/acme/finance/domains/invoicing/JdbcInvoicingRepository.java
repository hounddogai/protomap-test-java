package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the invoicing domain. Tenant keys scope every write.
 */
@Repository
public class JdbcInvoicingRepository implements InvoicingRepository {
    private final JdbcTemplate jdbc;

    private final EInvoiceRecordMapper eInvoiceMapper;

    private final InvoiceBatchRecordMapper invoiceBatchMapper;

    private final InvoiceTemplateRecordMapper invoiceTemplateMapper;

    public JdbcInvoicingRepository(
            JdbcTemplate jdbc,
            EInvoiceRecordMapper eInvoiceMapper,
            InvoiceBatchRecordMapper invoiceBatchMapper,
            InvoiceTemplateRecordMapper invoiceTemplateMapper) {
        this.jdbc = jdbc;
        this.eInvoiceMapper = eInvoiceMapper;
        this.invoiceBatchMapper = invoiceBatchMapper;
        this.invoiceTemplateMapper = invoiceTemplateMapper;
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
    public void saveEInvoice(String tenant, EInvoice value) {
        var record = eInvoiceMapper.toRecord(value);
        save(tenant, "EInvoice", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveInvoiceBatch(String tenant, InvoiceBatch value) {
        var record = invoiceBatchMapper.toRecord(value);
        save(tenant, "InvoiceBatch", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {
        var record = invoiceTemplateMapper.toRecord(value);
        save(tenant, "InvoiceTemplate", record.id(), record.label(), record.snapshot());
    }
}
