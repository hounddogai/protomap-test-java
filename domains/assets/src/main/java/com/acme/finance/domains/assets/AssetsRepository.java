package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.FixedAsset;

/**
 * The domain's durable record and audit port.
 */
public interface AssetsRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveAssetTransfer(String tenant, AssetTransfer value);

    void saveDepreciation(String tenant, Depreciation value);

    void saveDisposal(String tenant, Disposal value);

    void saveFixedAsset(String tenant, FixedAsset value);
}
