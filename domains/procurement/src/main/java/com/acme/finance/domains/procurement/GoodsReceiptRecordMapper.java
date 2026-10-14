package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.GoodsReceipt;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface GoodsReceiptRecordMapper {
    GoodsReceiptRecord toRecord(GoodsReceipt value);

    GoodsReceipt toProto(GoodsReceiptRecord value);
}
