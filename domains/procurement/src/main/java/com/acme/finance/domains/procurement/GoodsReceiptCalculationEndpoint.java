package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CalculateGoodsReceiptRequest;
import com.acme.procurement.v1.CalculateGoodsReceiptResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationRequest;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationResponse;
import com.acme.procurement.v1.GetGoodsReceiptRatesRequest;
import com.acme.procurement.v1.GetGoodsReceiptRatesResponse;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a goods receipt, such as totals, taxes, and estimates.
 */
@GrpcService
public class GoodsReceiptCalculationEndpoint extends GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceImplBase {
    protected final GoodsReceiptCalculationApplication application;

    public GoodsReceiptCalculationEndpoint(GoodsReceiptCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a goods receipt.
     */
    @Override
    public void calculateGoodsReceipt(CalculateGoodsReceiptRequest request, StreamObserver<CalculateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateGoodsReceipt(request));
    }

    /**
     * Estimates a goods receipt.
     */
    @Override
    public void estimateGoodsReceipt(EstimateGoodsReceiptRequest request, StreamObserver<EstimateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateGoodsReceipt(request));
    }

    /**
     * Quotes a goods receipt.
     */
    @Override
    public void quoteGoodsReceipt(QuoteGoodsReceiptRequest request, StreamObserver<QuoteGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteGoodsReceipt(request));
    }

    /**
     * Compares a goods receipt.
     */
    @Override
    public void compareGoodsReceiptOptions(CompareGoodsReceiptOptionsRequest request, StreamObserver<CompareGoodsReceiptOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareGoodsReceiptOptions(request));
    }

    /**
     * Recalculates a goods receipt.
     */
    @Override
    public void recalculateGoodsReceipts(RecalculateGoodsReceiptsRequest request, StreamObserver<RecalculateGoodsReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateGoodsReceipts(request));
    }

    /**
     * Simulates a goods receipt.
     */
    @Override
    public void simulateGoodsReceipt(SimulateGoodsReceiptRequest request, StreamObserver<SimulateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateGoodsReceipt(request));
    }

    /**
     * Explains a goods receipt.
     */
    @Override
    public void explainGoodsReceiptCalculation(ExplainGoodsReceiptCalculationRequest request, StreamObserver<ExplainGoodsReceiptCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainGoodsReceiptCalculation(request));
    }

    /**
     * Returns a goods receipt.
     */
    @Override
    public void getGoodsReceiptRates(GetGoodsReceiptRatesRequest request, StreamObserver<GetGoodsReceiptRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getGoodsReceiptRates(request));
    }
}
