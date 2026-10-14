package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierContract;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface SupplierContractRecordMapper {
    SupplierContractRecord toRecord(SupplierContract value);

    SupplierContract toProto(SupplierContractRecord value);
}
