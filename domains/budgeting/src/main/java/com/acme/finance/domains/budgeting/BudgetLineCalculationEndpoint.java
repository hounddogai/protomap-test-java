package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationResponse;
import com.acme.budgeting.v1.GetBudgetLineRatesRequest;
import com.acme.budgeting.v1.GetBudgetLineRatesResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a budget line, such as totals, taxes, and estimates.
 */
@GrpcService
public class BudgetLineCalculationEndpoint extends BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceImplBase {
    protected final BudgetLineCalculationApplication application;

    public BudgetLineCalculationEndpoint(BudgetLineCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a budget line.
     */
    @Override
    public void calculateBudgetLine(CalculateBudgetLineRequest request, StreamObserver<CalculateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateBudgetLine(request));
    }

    /**
     * Estimates a budget line.
     */
    @Override
    public void estimateBudgetLine(EstimateBudgetLineRequest request, StreamObserver<EstimateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateBudgetLine(request));
    }

    /**
     * Quotes a budget line.
     */
    @Override
    public void quoteBudgetLine(QuoteBudgetLineRequest request, StreamObserver<QuoteBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteBudgetLine(request));
    }

    /**
     * Compares a budget line.
     */
    @Override
    public void compareBudgetLineOptions(CompareBudgetLineOptionsRequest request, StreamObserver<CompareBudgetLineOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareBudgetLineOptions(request));
    }

    /**
     * Recalculates a budget line.
     */
    @Override
    public void recalculateBudgetLines(RecalculateBudgetLinesRequest request, StreamObserver<RecalculateBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateBudgetLines(request));
    }

    /**
     * Simulates a budget line.
     */
    @Override
    public void simulateBudgetLine(SimulateBudgetLineRequest request, StreamObserver<SimulateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateBudgetLine(request));
    }

    /**
     * Explains a budget line.
     */
    @Override
    public void explainBudgetLineCalculation(ExplainBudgetLineCalculationRequest request, StreamObserver<ExplainBudgetLineCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBudgetLineCalculation(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void getBudgetLineRates(GetBudgetLineRatesRequest request, StreamObserver<GetBudgetLineRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetLineRates(request));
    }
}
