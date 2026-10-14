package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.Rfq;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the procurement domain. Tenant keys scope every write.
 */
@Repository
public class JdbcProcurementRepository implements ProcurementRepository {
    private final JdbcTemplate jdbc;

    private final GoodsReceiptRecordMapper goodsReceiptMapper;

    private final PurchaseOrderRecordMapper purchaseOrderMapper;

    private final PurchaseRequisitionRecordMapper purchaseRequisitionMapper;

    private final RfqRecordMapper rfqMapper;

    public JdbcProcurementRepository(
            JdbcTemplate jdbc,
            GoodsReceiptRecordMapper goodsReceiptMapper,
            PurchaseOrderRecordMapper purchaseOrderMapper,
            PurchaseRequisitionRecordMapper purchaseRequisitionMapper,
            RfqRecordMapper rfqMapper) {
        this.jdbc = jdbc;
        this.goodsReceiptMapper = goodsReceiptMapper;
        this.purchaseOrderMapper = purchaseOrderMapper;
        this.purchaseRequisitionMapper = purchaseRequisitionMapper;
        this.rfqMapper = rfqMapper;
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
    public void saveGoodsReceipt(String tenant, GoodsReceipt value) {
        var record = goodsReceiptMapper.toRecord(value);
        save(tenant, "GoodsReceipt", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePurchaseOrder(String tenant, PurchaseOrder value) {
        var record = purchaseOrderMapper.toRecord(value);
        save(tenant, "PurchaseOrder", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {
        var record = purchaseRequisitionMapper.toRecord(value);
        save(tenant, "PurchaseRequisition", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRfq(String tenant, Rfq value) {
        var record = rfqMapper.toRecord(value);
        save(tenant, "Rfq", record.id(), record.label(), record.snapshot());
    }
}
