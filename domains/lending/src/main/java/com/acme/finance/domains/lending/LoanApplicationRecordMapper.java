package com.acme.finance.domains.lending;

import com.acme.lending.v1.LoanApplication;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface LoanApplicationRecordMapper {
    LoanApplicationRecord toRecord(LoanApplication value);

    LoanApplication toProto(LoanApplicationRecord value);
}
