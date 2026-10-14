package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.PayRun;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PayRunRecordMapper {
    PayRunRecord toRecord(PayRun value);

    PayRun toProto(PayRunRecord value);
}
