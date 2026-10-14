package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Control;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ControlRecordMapperImpl implements ControlRecordMapper {
    @Override
    public ControlRecord toRecord(Control value) {
        return new ControlRecord(value.getControlId(), value.getName(), value.toByteArray());
    }

    @Override
    public Control toProto(ControlRecord value) {
        try {
            return Control.parseFrom(value.snapshot()).toBuilder()
                    .setControlId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
