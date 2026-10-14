package com.acme.finance.domains.lending;

import com.acme.lending.v1.Repayment;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RepaymentRecordMapper {
    RepaymentRecord toRecord(Repayment value);

    Repayment toProto(RepaymentRecord value);
}
