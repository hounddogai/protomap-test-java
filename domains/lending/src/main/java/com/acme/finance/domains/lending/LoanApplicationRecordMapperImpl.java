package com.acme.finance.domains.lending;

import com.acme.lending.v1.LoanApplication;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class LoanApplicationRecordMapperImpl implements LoanApplicationRecordMapper {
    @Override
    public LoanApplicationRecord toRecord(LoanApplication value) {
        return new LoanApplicationRecord(value.getLoanApplicationId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public LoanApplication toProto(LoanApplicationRecord value) {
        try {
            return LoanApplication.parseFrom(value.snapshot()).toBuilder()
                    .setLoanApplicationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
