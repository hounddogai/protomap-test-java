package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.PostingBatch;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PostingBatchRecordMapperImpl implements PostingBatchRecordMapper {
    @Override
    public PostingBatchRecord toRecord(PostingBatch value) {
        return new PostingBatchRecord(value.getPostingBatchId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public PostingBatch toProto(PostingBatchRecord value) {
        try {
            return PostingBatch.parseFrom(value.snapshot()).toBuilder()
                    .setPostingBatchId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
