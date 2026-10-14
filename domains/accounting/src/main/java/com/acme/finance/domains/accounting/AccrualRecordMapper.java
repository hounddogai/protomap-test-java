package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AccrualRecordMapper {
    AccrualRecord toRecord(Accrual value);

    Accrual toProto(AccrualRecord value);
}
