package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierRisk;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface SupplierRiskRecordMapper {
    SupplierRiskRecord toRecord(SupplierRisk value);

    SupplierRisk toProto(SupplierRiskRecord value);
}
