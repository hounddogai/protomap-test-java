package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Allocation;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AllocationRecordMapper {
    AllocationRecord toRecord(Allocation value);

    Allocation toProto(AllocationRecord value);
}
