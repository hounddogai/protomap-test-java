package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.Sanction;

/**
 * The domain's durable record and audit port.
 */
public interface ComplianceRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveAttestation(String tenant, Attestation value);

    void saveComplianceCase(String tenant, ComplianceCase value);

    void saveControl(String tenant, Control value);

    void saveSanction(String tenant, Sanction value);
}
