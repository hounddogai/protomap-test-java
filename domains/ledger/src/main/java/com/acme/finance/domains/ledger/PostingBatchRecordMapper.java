package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.PostingBatch;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PostingBatchRecordMapper {
    PostingBatchRecord toRecord(PostingBatch value);

    PostingBatch toProto(PostingBatchRecord value);
}
