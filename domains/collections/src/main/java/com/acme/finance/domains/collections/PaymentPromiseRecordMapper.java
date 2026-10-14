package com.acme.finance.domains.collections;

import com.acme.collections.v1.PaymentPromise;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PaymentPromiseRecordMapper {
    PaymentPromiseRecord toRecord(PaymentPromise value);

    PaymentPromise toProto(PaymentPromiseRecord value);
}
