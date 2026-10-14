package com.acme.finance.domains.banking;

import com.acme.banking.v1.Statement;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class StatementRecordMapperImpl implements StatementRecordMapper {
    @Override
    public StatementRecord toRecord(Statement value) {
        return new StatementRecord(value.getStatementId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Statement toProto(StatementRecord value) {
        try {
            return Statement.parseFrom(value.snapshot()).toBuilder()
                    .setStatementId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
