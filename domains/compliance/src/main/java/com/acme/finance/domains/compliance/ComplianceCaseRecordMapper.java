package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ComplianceCase;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ComplianceCaseRecordMapper {
    ComplianceCaseRecord toRecord(ComplianceCase value);

    ComplianceCase toProto(ComplianceCaseRecord value);
}
