package com.acme.finance.domains.collections;

import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.WriteOff;

/**
 * The domain's durable record and audit port.
 */
public interface CollectionsRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveCollectionCase(String tenant, CollectionCase value);

    void savePaymentPromise(String tenant, PaymentPromise value);

    void saveWriteOff(String tenant, WriteOff value);
}
