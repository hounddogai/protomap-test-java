package com.acme.finance.domains.risk;

import com.acme.risk.v1.Exposure;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ExposureRecordMapperImpl implements ExposureRecordMapper {
    @Override
    public ExposureRecord toRecord(Exposure value) {
        return new ExposureRecord(value.getExposureId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public Exposure toProto(ExposureRecord value) {
        try {
            return Exposure.parseFrom(value.snapshot()).toBuilder()
                    .setExposureId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
