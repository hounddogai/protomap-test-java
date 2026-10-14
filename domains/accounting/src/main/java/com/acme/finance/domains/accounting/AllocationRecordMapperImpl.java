package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Allocation;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AllocationRecordMapperImpl implements AllocationRecordMapper {
    @Override
    public AllocationRecord toRecord(Allocation value) {
        return new AllocationRecord(value.getAllocationId(), value.getName(), value.toByteArray());
    }

    @Override
    public Allocation toProto(AllocationRecord value) {
        try {
            return Allocation.parseFrom(value.snapshot()).toBuilder()
                    .setAllocationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
