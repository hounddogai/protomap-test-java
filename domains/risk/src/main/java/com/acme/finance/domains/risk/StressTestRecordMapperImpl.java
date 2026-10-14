package com.acme.finance.domains.risk;

import com.acme.risk.v1.StressTest;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class StressTestRecordMapperImpl implements StressTestRecordMapper {
    @Override
    public StressTestRecord toRecord(StressTest value) {
        return new StressTestRecord(value.getStressTestId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public StressTest toProto(StressTestRecord value) {
        try {
            return StressTest.parseFrom(value.snapshot()).toBuilder()
                    .setStressTestId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
