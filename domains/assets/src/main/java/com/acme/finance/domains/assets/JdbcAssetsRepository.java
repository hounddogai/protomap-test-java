package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.FixedAsset;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the assets domain. Tenant keys scope every write.
 */
@Repository
public class JdbcAssetsRepository implements AssetsRepository {
    private final JdbcTemplate jdbc;

    private final AssetTransferRecordMapper assetTransferMapper;

    private final DepreciationRecordMapper depreciationMapper;

    private final DisposalRecordMapper disposalMapper;

    private final FixedAssetRecordMapper fixedAssetMapper;

    public JdbcAssetsRepository(
            JdbcTemplate jdbc,
            AssetTransferRecordMapper assetTransferMapper,
            DepreciationRecordMapper depreciationMapper,
            DisposalRecordMapper disposalMapper,
            FixedAssetRecordMapper fixedAssetMapper) {
        this.jdbc = jdbc;
        this.assetTransferMapper = assetTransferMapper;
        this.depreciationMapper = depreciationMapper;
        this.disposalMapper = disposalMapper;
        this.fixedAssetMapper = fixedAssetMapper;
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
    public void saveAssetTransfer(String tenant, AssetTransfer value) {
        var record = assetTransferMapper.toRecord(value);
        save(tenant, "AssetTransfer", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveDepreciation(String tenant, Depreciation value) {
        var record = depreciationMapper.toRecord(value);
        save(tenant, "Depreciation", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveDisposal(String tenant, Disposal value) {
        var record = disposalMapper.toRecord(value);
        save(tenant, "Disposal", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveFixedAsset(String tenant, FixedAsset value) {
        var record = fixedAssetMapper.toRecord(value);
        save(tenant, "FixedAsset", record.id(), record.label(), record.snapshot());
    }
}
