package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CompareEInvoiceOptionsRequest;
import com.acme.invoicing.v1.CompareEInvoiceOptionsResponse;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EstimateEInvoiceRequest;
import com.acme.invoicing.v1.EstimateEInvoiceResponse;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationRequest;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationResponse;
import com.acme.invoicing.v1.GetEInvoiceRatesRequest;
import com.acme.invoicing.v1.GetEInvoiceRatesResponse;
import com.acme.invoicing.v1.QuoteEInvoiceRequest;
import com.acme.invoicing.v1.QuoteEInvoiceResponse;
import com.acme.invoicing.v1.RecalculateEInvoicesRequest;
import com.acme.invoicing.v1.RecalculateEInvoicesResponse;
import com.acme.invoicing.v1.SimulateEInvoiceRequest;
import com.acme.invoicing.v1.SimulateEInvoiceResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an e invoice, such as totals, taxes, and estimates.
 */
@GrpcService
public class EInvoiceCalculationEndpoint extends EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceImplBase {
    protected final EInvoiceCalculationApplication application;

    public EInvoiceCalculationEndpoint(EInvoiceCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an e invoice.
     */
    @Override
    public void calculateEInvoice(CalculateEInvoiceRequest request, StreamObserver<CalculateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateEInvoice(request));
    }

    /**
     * Estimates an e invoice.
     */
    @Override
    public void estimateEInvoice(EstimateEInvoiceRequest request, StreamObserver<EstimateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateEInvoice(request));
    }

    /**
     * Quotes an e invoice.
     */
    @Override
    public void quoteEInvoice(QuoteEInvoiceRequest request, StreamObserver<QuoteEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteEInvoice(request));
    }

    /**
     * Compares an e invoice.
     */
    @Override
    public void compareEInvoiceOptions(CompareEInvoiceOptionsRequest request, StreamObserver<CompareEInvoiceOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareEInvoiceOptions(request));
    }

    /**
     * Recalculates an e invoice.
     */
    @Override
    public void recalculateEInvoices(RecalculateEInvoicesRequest request, StreamObserver<RecalculateEInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateEInvoices(request));
    }

    /**
     * Simulates an e invoice.
     */
    @Override
    public void simulateEInvoice(SimulateEInvoiceRequest request, StreamObserver<SimulateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateEInvoice(request));
    }

    /**
     * Explains an e invoice.
     */
    @Override
    public void explainEInvoiceCalculation(ExplainEInvoiceCalculationRequest request, StreamObserver<ExplainEInvoiceCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainEInvoiceCalculation(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void getEInvoiceRates(GetEInvoiceRatesRequest request, StreamObserver<GetEInvoiceRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getEInvoiceRates(request));
    }
}
