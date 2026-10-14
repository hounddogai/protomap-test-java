package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.EstimateInvoiceBatchRequest;
import com.acme.invoicing.v1.EstimateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.SimulateInvoiceBatchRequest;
import com.acme.invoicing.v1.SimulateInvoiceBatchResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an invoice batch, such as totals, taxes, and estimates.
 */
@GrpcService
public class InvoiceBatchCalculationEndpoint extends InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceImplBase {
    protected final InvoiceBatchCalculationApplication application;

    public InvoiceBatchCalculationEndpoint(InvoiceBatchCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an invoice batch.
     */
    @Override
    public void calculateInvoiceBatch(CalculateInvoiceBatchRequest request, StreamObserver<CalculateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateInvoiceBatch(request));
    }

    /**
     * Estimates an invoice batch.
     */
    @Override
    public void estimateInvoiceBatch(EstimateInvoiceBatchRequest request, StreamObserver<EstimateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateInvoiceBatch(request));
    }

    /**
     * Quotes an invoice batch.
     */
    @Override
    public void quoteInvoiceBatch(QuoteInvoiceBatchRequest request, StreamObserver<QuoteInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteInvoiceBatch(request));
    }

    /**
     * Compares an invoice batch.
     */
    @Override
    public void compareInvoiceBatchOptions(CompareInvoiceBatchOptionsRequest request, StreamObserver<CompareInvoiceBatchOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareInvoiceBatchOptions(request));
    }

    /**
     * Recalculates an invoice batch.
     */
    @Override
    public void recalculateInvoiceBatches(RecalculateInvoiceBatchesRequest request, StreamObserver<RecalculateInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateInvoiceBatches(request));
    }

    /**
     * Simulates an invoice batch.
     */
    @Override
    public void simulateInvoiceBatch(SimulateInvoiceBatchRequest request, StreamObserver<SimulateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateInvoiceBatch(request));
    }

    /**
     * Explains an invoice batch.
     */
    @Override
    public void explainInvoiceBatchCalculation(ExplainInvoiceBatchCalculationRequest request, StreamObserver<ExplainInvoiceBatchCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceBatchCalculation(request));
    }

    /**
     * Returns an invoice batch.
     */
    @Override
    public void getInvoiceBatchRates(GetInvoiceBatchRatesRequest request, StreamObserver<GetInvoiceBatchRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceBatchRates(request));
    }
}
