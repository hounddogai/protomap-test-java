package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a budget, such as totals, taxes, and estimates.
 */
public abstract class AbstractBudgetCalculationEndpoint extends BudgetCalculationServiceGrpc.BudgetCalculationServiceImplBase {
    protected final BudgetCalculationApplication application;

    protected AbstractBudgetCalculationEndpoint(BudgetCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a budget.
     */
    @Override
    public void calculateBudget(CalculateBudgetRequest request, StreamObserver<CalculateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateBudget(request));
    }

    /**
     * Estimates a budget.
     */
    @Override
    public void estimateBudget(EstimateBudgetRequest request, StreamObserver<EstimateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateBudget(request));
    }

    /**
     * Quotes a budget.
     */
    @Override
    public void quoteBudget(QuoteBudgetRequest request, StreamObserver<QuoteBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteBudget(request));
    }

    /**
     * Compares a budget.
     */
    @Override
    public void compareBudgetOptions(CompareBudgetOptionsRequest request, StreamObserver<CompareBudgetOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareBudgetOptions(request));
    }

    /**
     * Recalculates a budget.
     */
    @Override
    public void recalculateBudgets(RecalculateBudgetsRequest request, StreamObserver<RecalculateBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateBudgets(request));
    }

    /**
     * Simulates a budget.
     */
    @Override
    public void simulateBudget(SimulateBudgetRequest request, StreamObserver<SimulateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateBudget(request));
    }

    /**
     * Explains a budget.
     */
    @Override
    public void explainBudgetCalculation(ExplainBudgetCalculationRequest request, StreamObserver<ExplainBudgetCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBudgetCalculation(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void getBudgetRates(GetBudgetRatesRequest request, StreamObserver<GetBudgetRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetRates(request));
    }
}
