package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRisk;

/**
 * The domain's durable record and audit port.
 */
public interface SuppliersRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveSupplier(String tenant, Supplier value);

    void saveSupplierContract(String tenant, SupplierContract value);

    void saveSupplierRating(String tenant, SupplierRating value);

    void saveSupplierRisk(String tenant, SupplierRisk value);
}
