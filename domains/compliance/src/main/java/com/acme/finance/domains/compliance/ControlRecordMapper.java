package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Control;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ControlRecordMapper {
    ControlRecord toRecord(Control value);

    Control toProto(ControlRecord value);
}
