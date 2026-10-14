package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AuditEventRecordMapper {
    AuditEventRecord toRecord(AuditEvent value);

    AuditEvent toProto(AuditEventRecord value);
}
