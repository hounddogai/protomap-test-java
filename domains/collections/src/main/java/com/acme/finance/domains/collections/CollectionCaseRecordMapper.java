package com.acme.finance.domains.collections;

import com.acme.collections.v1.CollectionCase;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CollectionCaseRecordMapper {
    CollectionCaseRecord toRecord(CollectionCase value);

    CollectionCase toProto(CollectionCaseRecord value);
}
