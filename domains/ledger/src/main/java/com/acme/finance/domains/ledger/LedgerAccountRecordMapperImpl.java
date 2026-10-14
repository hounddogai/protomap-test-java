package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.LedgerAccount;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class LedgerAccountRecordMapperImpl implements LedgerAccountRecordMapper {
    @Override
    public LedgerAccountRecord toRecord(LedgerAccount value) {
        return new LedgerAccountRecord(value.getLedgerAccountId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public LedgerAccount toProto(LedgerAccountRecord value) {
        try {
            return LedgerAccount.parseFrom(value.snapshot()).toBuilder()
                    .setLedgerAccountId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
