package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Attestation;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AttestationRecordMapper {
    AttestationRecord toRecord(Attestation value);

    Attestation toProto(AttestationRecord value);
}
