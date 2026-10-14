package com.acme.finance.domains.lending;

import com.acme.lending.v1.Loan;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface LoanRecordMapper {
    LoanRecord toRecord(Loan value);

    Loan toProto(LoanRecord value);
}
