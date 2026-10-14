package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationRequest;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationResponse;
import com.acme.expenses.v1.GetExpensePolicyRatesRequest;
import com.acme.expenses.v1.GetExpensePolicyRatesResponse;
import com.acme.expenses.v1.QuoteExpensePolicyRequest;
import com.acme.expenses.v1.QuoteExpensePolicyResponse;
import com.acme.expenses.v1.RecalculateExpensePoliciesRequest;
import com.acme.expenses.v1.RecalculateExpensePoliciesResponse;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an expense policy, such as totals, taxes, and estimates.
 */
@GrpcService
public class ExpensePolicyCalculationEndpoint extends ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceImplBase {
    protected final ExpensePolicyCalculationApplication application;

    public ExpensePolicyCalculationEndpoint(ExpensePolicyCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an expense policy.
     */
    @Override
    public void calculateExpensePolicy(CalculateExpensePolicyRequest request, StreamObserver<CalculateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateExpensePolicy(request));
    }

    /**
     * Estimates an expense policy.
     */
    @Override
    public void estimateExpensePolicy(EstimateExpensePolicyRequest request, StreamObserver<EstimateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateExpensePolicy(request));
    }

    /**
     * Quotes an expense policy.
     */
    @Override
    public void quoteExpensePolicy(QuoteExpensePolicyRequest request, StreamObserver<QuoteExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteExpensePolicy(request));
    }

    /**
     * Recalculates an expense policy.
     */
    @Override
    public void recalculateExpensePolicies(RecalculateExpensePoliciesRequest request, StreamObserver<RecalculateExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateExpensePolicies(request));
    }

    /**
     * Simulates an expense policy.
     */
    @Override
    public void simulateExpensePolicy(SimulateExpensePolicyRequest request, StreamObserver<SimulateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateExpensePolicy(request));
    }

    /**
     * Explains an expense policy.
     */
    @Override
    public void explainExpensePolicyCalculation(ExplainExpensePolicyCalculationRequest request, StreamObserver<ExplainExpensePolicyCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExpensePolicyCalculation(request));
    }

    /**
     * Returns an expense policy.
     */
    @Override
    public void getExpensePolicyRates(GetExpensePolicyRatesRequest request, StreamObserver<GetExpensePolicyRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpensePolicyRates(request));
    }
}
