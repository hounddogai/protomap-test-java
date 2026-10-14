package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.Supplier;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface SupplierRecordMapper {
    SupplierRecord toRecord(Supplier value);

    Supplier toProto(SupplierRecord value);
}
