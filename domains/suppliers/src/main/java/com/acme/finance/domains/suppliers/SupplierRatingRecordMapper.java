package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierRating;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface SupplierRatingRecordMapper {
    SupplierRatingRecord toRecord(SupplierRating value);

    SupplierRating toProto(SupplierRatingRecord value);
}
