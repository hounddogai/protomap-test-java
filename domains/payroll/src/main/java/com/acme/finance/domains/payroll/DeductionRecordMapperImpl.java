package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.Deduction;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class DeductionRecordMapperImpl implements DeductionRecordMapper {
    @Override
    public DeductionRecord toRecord(Deduction value) {
        return new DeductionRecord(value.getDeductionId(), value.getName(), value.toByteArray());
    }

    @Override
    public Deduction toProto(DeductionRecord value) {
        try {
            return Deduction.parseFrom(value.snapshot()).toBuilder()
                    .setDeductionId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
