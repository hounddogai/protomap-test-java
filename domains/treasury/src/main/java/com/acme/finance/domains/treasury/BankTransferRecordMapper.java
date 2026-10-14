package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.BankTransfer;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface BankTransferRecordMapper {
    BankTransferRecord toRecord(BankTransfer value);

    BankTransfer toProto(BankTransferRecord value);
}
