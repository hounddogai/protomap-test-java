package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.PurchaseRequisition;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PurchaseRequisitionRecordMapper {
    PurchaseRequisitionRecord toRecord(PurchaseRequisition value);

    PurchaseRequisition toProto(PurchaseRequisitionRecord value);
}
