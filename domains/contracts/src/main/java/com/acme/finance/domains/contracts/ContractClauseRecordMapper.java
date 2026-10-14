package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ContractClause;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ContractClauseRecordMapper {
    ContractClauseRecord toRecord(ContractClause value);

    ContractClause toProto(ContractClauseRecord value);
}
