package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AuditEventRecordMapperImpl implements AuditEventRecordMapper {
    @Override
    public AuditEventRecord toRecord(AuditEvent value) {
        return new AuditEventRecord(value.getAuditEventId(), value.getKind(), value.toByteArray());
    }

    @Override
    public AuditEvent toProto(AuditEventRecord value) {
        try {
            return AuditEvent.parseFrom(value.snapshot()).toBuilder()
                    .setAuditEventId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
