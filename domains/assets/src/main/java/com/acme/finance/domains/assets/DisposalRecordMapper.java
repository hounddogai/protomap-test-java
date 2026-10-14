package com.acme.finance.domains.assets;

import com.acme.assets.v1.Disposal;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface DisposalRecordMapper {
    DisposalRecord toRecord(Disposal value);

    Disposal toProto(DisposalRecord value);
}
