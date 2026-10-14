package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.PurchaseOrder;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PurchaseOrderRecordMapper {
    PurchaseOrderRecord toRecord(PurchaseOrder value);

    PurchaseOrder toProto(PurchaseOrderRecord value);
}
