package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.RetentionPolicy;

/**
 * The domain's durable record and audit port.
 */
public interface AuditRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveAuditEvent(String tenant, AuditEvent value);

    void saveAuditTrail(String tenant, AuditTrail value);

    void saveRetentionPolicy(String tenant, RetentionPolicy value);
}
