package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface BankAccountRecordMapper {
    BankAccountRecord toRecord(BankAccount value);

    BankAccount toProto(BankAccountRecord value);
}
