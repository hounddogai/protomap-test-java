package com.acme.finance.domains.collections;

import com.acme.collections.v1.WriteOff;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class WriteOffRecordMapperImpl implements WriteOffRecordMapper {
    @Override
    public WriteOffRecord toRecord(WriteOff value) {
        return new WriteOffRecord(value.getWriteOffId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public WriteOff toProto(WriteOffRecord value) {
        try {
            return WriteOff.parseFrom(value.snapshot()).toBuilder()
                    .setWriteOffId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
