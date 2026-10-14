package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpensePolicy;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ExpensePolicyRecordMapperImpl implements ExpensePolicyRecordMapper {
    @Override
    public ExpensePolicyRecord toRecord(ExpensePolicy value) {
        return new ExpensePolicyRecord(value.getExpensePolicyId(), value.getName(), value.toByteArray());
    }

    @Override
    public ExpensePolicy toProto(ExpensePolicyRecord value) {
        try {
            return ExpensePolicy.parseFrom(value.snapshot()).toBuilder()
                    .setExpensePolicyId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
