package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.LedgerAccount;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface LedgerAccountRecordMapper {
    LedgerAccountRecord toRecord(LedgerAccount value);

    LedgerAccount toProto(LedgerAccountRecord value);
}
