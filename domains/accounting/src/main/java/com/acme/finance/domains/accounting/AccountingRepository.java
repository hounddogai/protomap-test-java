package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.Reconciliation;

/**
 * The domain's durable record and audit port.
 */
public interface AccountingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveAccrual(String tenant, Accrual value);

    void saveAllocation(String tenant, Allocation value);

    void saveChartOfAccounts(String tenant, ChartOfAccounts value);

    void saveReconciliation(String tenant, Reconciliation value);
}
