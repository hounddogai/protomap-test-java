package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Reconciliation;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ReconciliationRecordMapper {
    ReconciliationRecord toRecord(Reconciliation value);

    Reconciliation toProto(ReconciliationRecord value);
}
