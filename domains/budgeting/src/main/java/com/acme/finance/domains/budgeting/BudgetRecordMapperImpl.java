package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Budget;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class BudgetRecordMapperImpl implements BudgetRecordMapper {
    @Override
    public BudgetRecord toRecord(Budget value) {
        return new BudgetRecord(value.getBudgetId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public Budget toProto(BudgetRecord value) {
        try {
            return Budget.parseFrom(value.snapshot()).toBuilder()
                    .setBudgetId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
