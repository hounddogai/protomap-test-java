package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.Rfq;

/**
 * The domain's durable record and audit port.
 */
public interface ProcurementRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveGoodsReceipt(String tenant, GoodsReceipt value);

    void savePurchaseOrder(String tenant, PurchaseOrder value);

    void savePurchaseRequisition(String tenant, PurchaseRequisition value);

    void saveRfq(String tenant, Rfq value);
}
