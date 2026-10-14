package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.TaxWithholding;

/**
 * The domain's durable record and audit port.
 */
public interface PayrollRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveDeduction(String tenant, Deduction value);

    void savePayRun(String tenant, PayRun value);

    void savePayStub(String tenant, PayStub value);

    void saveTaxWithholding(String tenant, TaxWithholding value);
}
