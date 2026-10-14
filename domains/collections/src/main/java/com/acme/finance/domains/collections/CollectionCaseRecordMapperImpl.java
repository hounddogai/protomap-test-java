package com.acme.finance.domains.collections;

import com.acme.collections.v1.CollectionCase;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CollectionCaseRecordMapperImpl implements CollectionCaseRecordMapper {
    @Override
    public CollectionCaseRecord toRecord(CollectionCase value) {
        return new CollectionCaseRecord(value.getCollectionCaseId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public CollectionCase toProto(CollectionCaseRecord value) {
        try {
            return CollectionCase.parseFrom(value.snapshot()).toBuilder()
                    .setCollectionCaseId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
