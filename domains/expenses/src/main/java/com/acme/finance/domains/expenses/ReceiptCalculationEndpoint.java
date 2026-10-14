package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CompareReceiptOptionsRequest;
import com.acme.expenses.v1.CompareReceiptOptionsResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExplainReceiptCalculationRequest;
import com.acme.expenses.v1.ExplainReceiptCalculationResponse;
import com.acme.expenses.v1.GetReceiptRatesRequest;
import com.acme.expenses.v1.GetReceiptRatesResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a receipt, such as totals, taxes, and estimates.
 */
@GrpcService
public class ReceiptCalculationEndpoint extends ReceiptCalculationServiceGrpc.ReceiptCalculationServiceImplBase {
    protected final ReceiptCalculationApplication application;

    public ReceiptCalculationEndpoint(ReceiptCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a receipt.
     */
    @Override
    public void calculateReceipt(CalculateReceiptRequest request, StreamObserver<CalculateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateReceipt(request));
    }

    /**
     * Estimates a receipt.
     */
    @Override
    public void estimateReceipt(EstimateReceiptRequest request, StreamObserver<EstimateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateReceipt(request));
    }

    /**
     * Compares a receipt.
     */
    @Override
    public void compareReceiptOptions(CompareReceiptOptionsRequest request, StreamObserver<CompareReceiptOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareReceiptOptions(request));
    }

    /**
     * Recalculates a receipt.
     */
    @Override
    public void recalculateReceipts(RecalculateReceiptsRequest request, StreamObserver<RecalculateReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateReceipts(request));
    }

    /**
     * Simulates a receipt.
     */
    @Override
    public void simulateReceipt(SimulateReceiptRequest request, StreamObserver<SimulateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateReceipt(request));
    }

    /**
     * Explains a receipt.
     */
    @Override
    public void explainReceiptCalculation(ExplainReceiptCalculationRequest request, StreamObserver<ExplainReceiptCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainReceiptCalculation(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void getReceiptRates(GetReceiptRatesRequest request, StreamObserver<GetReceiptRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReceiptRates(request));
    }
}
