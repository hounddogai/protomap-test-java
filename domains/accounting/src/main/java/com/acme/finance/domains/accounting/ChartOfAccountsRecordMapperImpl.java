package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ChartOfAccounts;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ChartOfAccountsRecordMapperImpl implements ChartOfAccountsRecordMapper {
    @Override
    public ChartOfAccountsRecord toRecord(ChartOfAccounts value) {
        return new ChartOfAccountsRecord(value.getChartOfAccountsId(), value.getName(), value.toByteArray());
    }

    @Override
    public ChartOfAccounts toProto(ChartOfAccountsRecord value) {
        try {
            return ChartOfAccounts.parseFrom(value.snapshot()).toBuilder()
                    .setChartOfAccountsId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
