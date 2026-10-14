package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLine;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class BudgetLineRecordMapperImpl implements BudgetLineRecordMapper {
    @Override
    public BudgetLineRecord toRecord(BudgetLine value) {
        return new BudgetLineRecord(value.getBudgetLineId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public BudgetLine toProto(BudgetLineRecord value) {
        try {
            return BudgetLine.parseFrom(value.snapshot()).toBuilder()
                    .setBudgetLineId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
