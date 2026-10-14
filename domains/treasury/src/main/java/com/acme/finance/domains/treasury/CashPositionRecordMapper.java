package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.CashPosition;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CashPositionRecordMapper {
    CashPositionRecord toRecord(CashPosition value);

    CashPosition toProto(CashPositionRecord value);
}
