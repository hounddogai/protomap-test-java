package com.acme.finance.domains.audit;

import com.acme.audit.v1.RetentionPolicy;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RetentionPolicyRecordMapper {
    RetentionPolicyRecord toRecord(RetentionPolicy value);

    RetentionPolicy toProto(RetentionPolicyRecord value);
}
