package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CalculateInvoiceTemplateRequest;
import com.acme.invoicing.v1.CalculateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EstimateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EstimateInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRatesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRatesResponse;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an invoice template, such as totals, taxes, and estimates.
 */
@GrpcService
public class InvoiceTemplateCalculationEndpoint extends InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceImplBase {
    protected final InvoiceTemplateCalculationApplication application;

    public InvoiceTemplateCalculationEndpoint(InvoiceTemplateCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an invoice template.
     */
    @Override
    public void calculateInvoiceTemplate(CalculateInvoiceTemplateRequest request, StreamObserver<CalculateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateInvoiceTemplate(request));
    }

    /**
     * Estimates an invoice template.
     */
    @Override
    public void estimateInvoiceTemplate(EstimateInvoiceTemplateRequest request, StreamObserver<EstimateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateInvoiceTemplate(request));
    }

    /**
     * Quotes an invoice template.
     */
    @Override
    public void quoteInvoiceTemplate(QuoteInvoiceTemplateRequest request, StreamObserver<QuoteInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteInvoiceTemplate(request));
    }

    /**
     * Compares an invoice template.
     */
    @Override
    public void compareInvoiceTemplateOptions(CompareInvoiceTemplateOptionsRequest request, StreamObserver<CompareInvoiceTemplateOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareInvoiceTemplateOptions(request));
    }

    /**
     * Simulates an invoice template.
     */
    @Override
    public void simulateInvoiceTemplate(SimulateInvoiceTemplateRequest request, StreamObserver<SimulateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateInvoiceTemplate(request));
    }

    /**
     * Explains an invoice template.
     */
    @Override
    public void explainInvoiceTemplateCalculation(ExplainInvoiceTemplateCalculationRequest request, StreamObserver<ExplainInvoiceTemplateCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceTemplateCalculation(request));
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void getInvoiceTemplateRates(GetInvoiceTemplateRatesRequest request, StreamObserver<GetInvoiceTemplateRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceTemplateRates(request));
    }
}
