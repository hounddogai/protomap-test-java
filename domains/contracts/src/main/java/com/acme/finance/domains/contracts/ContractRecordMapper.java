package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Contract;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ContractRecordMapper {
    ContractRecord toRecord(Contract value);

    Contract toProto(ContractRecord value);
}
