package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.FiscalPeriod;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface FiscalPeriodRecordMapper {
    FiscalPeriodRecord toRecord(FiscalPeriod value);

    FiscalPeriod toProto(FiscalPeriodRecord value);
}
