package com.acme.finance.domains.collections;

import com.acme.collections.v1.WriteOff;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface WriteOffRecordMapper {
    WriteOffRecord toRecord(WriteOff value);

    WriteOff toProto(WriteOffRecord value);
}
