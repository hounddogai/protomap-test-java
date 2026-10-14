package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.PayStub;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PayStubRecordMapper {
    PayStubRecord toRecord(PayStub value);

    PayStub toProto(PayStubRecord value);
}
