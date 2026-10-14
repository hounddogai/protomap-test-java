package com.acme.finance.domains.banking;

import com.acme.banking.v1.Statement;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface StatementRecordMapper {
    StatementRecord toRecord(Statement value);

    Statement toProto(StatementRecord value);
}
