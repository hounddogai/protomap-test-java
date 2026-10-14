package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AuditTrailRecordMapperImpl implements AuditTrailRecordMapper {
    @Override
    public AuditTrailRecord toRecord(AuditTrail value) {
        return new AuditTrailRecord(value.getAuditTrailId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public AuditTrail toProto(AuditTrailRecord value) {
        try {
            return AuditTrail.parseFrom(value.snapshot()).toBuilder()
                    .setAuditTrailId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
