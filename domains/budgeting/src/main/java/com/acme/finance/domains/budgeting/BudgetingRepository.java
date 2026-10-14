package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.Variance;

/**
 * The domain's durable record and audit port.
 */
public interface BudgetingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveBudget(String tenant, Budget value);

    void saveBudgetLine(String tenant, BudgetLine value);

    void saveForecast(String tenant, Forecast value);

    void saveVariance(String tenant, Variance value);
}
