package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Policy;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PolicyRecordMapperImpl implements PolicyRecordMapper {
    @Override
    public PolicyRecord toRecord(Policy value) {
        return new PolicyRecord(value.getPolicyId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public Policy toProto(PolicyRecord value) {
        try {
            return Policy.parseFrom(value.snapshot()).toBuilder()
                    .setPolicyId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
