package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.GetReconciliationRatesRequest;
import com.acme.accounting.v1.GetReconciliationRatesResponse;
import com.acme.accounting.v1.QuoteReconciliationRequest;
import com.acme.accounting.v1.QuoteReconciliationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.SimulateReconciliationRequest;
import com.acme.accounting.v1.SimulateReconciliationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a reconciliation, such as totals, taxes, and estimates.
 */
@GrpcService
public class ReconciliationCalculationEndpoint extends ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceImplBase {
    protected final ReconciliationCalculationApplication application;

    public ReconciliationCalculationEndpoint(ReconciliationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a reconciliation.
     */
    @Override
    public void calculateReconciliation(CalculateReconciliationRequest request, StreamObserver<CalculateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateReconciliation(request));
    }

    /**
     * Estimates a reconciliation.
     */
    @Override
    public void estimateReconciliation(EstimateReconciliationRequest request, StreamObserver<EstimateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateReconciliation(request));
    }

    /**
     * Quotes a reconciliation.
     */
    @Override
    public void quoteReconciliation(QuoteReconciliationRequest request, StreamObserver<QuoteReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteReconciliation(request));
    }

    /**
     * Compares a reconciliation.
     */
    @Override
    public void compareReconciliationOptions(CompareReconciliationOptionsRequest request, StreamObserver<CompareReconciliationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareReconciliationOptions(request));
    }

    /**
     * Recalculates a reconciliation.
     */
    @Override
    public void recalculateReconciliations(RecalculateReconciliationsRequest request, StreamObserver<RecalculateReconciliationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateReconciliations(request));
    }

    /**
     * Simulates a reconciliation.
     */
    @Override
    public void simulateReconciliation(SimulateReconciliationRequest request, StreamObserver<SimulateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateReconciliation(request));
    }

    /**
     * Explains a reconciliation.
     */
    @Override
    public void explainReconciliationCalculation(ExplainReconciliationCalculationRequest request, StreamObserver<ExplainReconciliationCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainReconciliationCalculation(request));
    }

    /**
     * Returns a reconciliation.
     */
    @Override
    public void getReconciliationRates(GetReconciliationRatesRequest request, StreamObserver<GetReconciliationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReconciliationRates(request));
    }
}
