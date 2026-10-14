package com.acme.finance.domains.lending;

import com.acme.lending.v1.Loan;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class LoanRecordMapperImpl implements LoanRecordMapper {
    @Override
    public LoanRecord toRecord(Loan value) {
        return new LoanRecord(value.getLoanId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public Loan toProto(LoanRecord value) {
        try {
            return Loan.parseFrom(value.snapshot()).toBuilder()
                    .setLoanId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
