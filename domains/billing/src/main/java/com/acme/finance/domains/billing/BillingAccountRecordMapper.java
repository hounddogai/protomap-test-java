package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccount;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface BillingAccountRecordMapper {
    BillingAccountRecord toRecord(BillingAccount value);

    BillingAccount toProto(BillingAccountRecord value);
}
