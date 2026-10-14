package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.TaxWithholding;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface TaxWithholdingRecordMapper {
    TaxWithholdingRecord toRecord(TaxWithholding value);

    TaxWithholding toProto(TaxWithholdingRecord value);
}
