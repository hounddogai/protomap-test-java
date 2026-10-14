package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.FxHedge;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface FxHedgeRecordMapper {
    FxHedgeRecord toRecord(FxHedge value);

    FxHedge toProto(FxHedgeRecord value);
}
