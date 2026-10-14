package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.PayRun;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PayRunRecordMapperImpl implements PayRunRecordMapper {
    @Override
    public PayRunRecord toRecord(PayRun value) {
        return new PayRunRecord(value.getPayRunId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public PayRun toProto(PayRunRecord value) {
        try {
            return PayRun.parseFrom(value.snapshot()).toBuilder()
                    .setPayRunId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
