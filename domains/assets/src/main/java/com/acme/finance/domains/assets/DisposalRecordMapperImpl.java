package com.acme.finance.domains.assets;

import com.acme.assets.v1.Disposal;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class DisposalRecordMapperImpl implements DisposalRecordMapper {
    @Override
    public DisposalRecord toRecord(Disposal value) {
        return new DisposalRecord(value.getDisposalId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Disposal toProto(DisposalRecord value) {
        try {
            return Disposal.parseFrom(value.snapshot()).toBuilder()
                    .setDisposalId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
