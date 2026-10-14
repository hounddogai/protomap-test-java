package com.acme.finance.domains.audit;

import com.acme.audit.v1.RetentionPolicy;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RetentionPolicyRecordMapperImpl implements RetentionPolicyRecordMapper {
    @Override
    public RetentionPolicyRecord toRecord(RetentionPolicy value) {
        return new RetentionPolicyRecord(value.getRetentionPolicyId(), value.getName(), value.toByteArray());
    }

    @Override
    public RetentionPolicy toProto(RetentionPolicyRecord value) {
        try {
            return RetentionPolicy.parseFrom(value.snapshot()).toBuilder()
                    .setRetentionPolicyId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
