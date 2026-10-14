package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AuditTrailRecordMapper {
    AuditTrailRecord toRecord(AuditTrail value);

    AuditTrail toProto(AuditTrailRecord value);
}
