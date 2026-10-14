package com.acme.finance.domains.billing;

import com.acme.billing.v1.PaymentTerm;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PaymentTermRecordMapper {
    PaymentTermRecord toRecord(PaymentTerm value);

    PaymentTerm toProto(PaymentTermRecord value);
}
