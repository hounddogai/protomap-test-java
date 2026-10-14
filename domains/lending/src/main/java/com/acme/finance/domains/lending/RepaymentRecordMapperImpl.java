package com.acme.finance.domains.lending;

import com.acme.lending.v1.Repayment;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RepaymentRecordMapperImpl implements RepaymentRecordMapper {
    @Override
    public RepaymentRecord toRecord(Repayment value) {
        return new RepaymentRecord(value.getRepaymentId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public Repayment toProto(RepaymentRecord value) {
        try {
            return Repayment.parseFrom(value.snapshot()).toBuilder()
                    .setRepaymentId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
