package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.Receipt;

/**
 * The domain's durable record and audit port.
 */
public interface ExpensesRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveExpensePolicy(String tenant, ExpensePolicy value);

    void saveExpenseReport(String tenant, ExpenseReport value);

    void saveMileageClaim(String tenant, MileageClaim value);

    void saveReceipt(String tenant, Receipt value);
}
