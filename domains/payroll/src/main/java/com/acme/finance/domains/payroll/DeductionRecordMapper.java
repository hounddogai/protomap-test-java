package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.Deduction;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface DeductionRecordMapper {
    DeductionRecord toRecord(Deduction value);

    Deduction toProto(DeductionRecord value);
}
