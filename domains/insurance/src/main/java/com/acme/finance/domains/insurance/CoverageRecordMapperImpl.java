package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Coverage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CoverageRecordMapperImpl implements CoverageRecordMapper {
    @Override
    public CoverageRecord toRecord(Coverage value) {
        return new CoverageRecord(value.getCoverageId(), value.getName(), value.toByteArray());
    }

    @Override
    public Coverage toProto(CoverageRecord value) {
        try {
            return Coverage.parseFrom(value.snapshot()).toBuilder()
                    .setCoverageId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
