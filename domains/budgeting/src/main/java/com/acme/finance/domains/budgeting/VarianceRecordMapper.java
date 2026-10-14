package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Variance;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface VarianceRecordMapper {
    VarianceRecord toRecord(Variance value);

    Variance toProto(VarianceRecord value);
}
