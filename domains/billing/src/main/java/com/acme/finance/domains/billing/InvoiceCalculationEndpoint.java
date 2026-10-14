package com.acme.finance.domains.billing;

import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CompareInvoiceOptionsRequest;
import com.acme.billing.v1.CompareInvoiceOptionsResponse;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.ExplainInvoiceCalculationRequest;
import com.acme.billing.v1.ExplainInvoiceCalculationResponse;
import com.acme.billing.v1.GetInvoiceRatesRequest;
import com.acme.billing.v1.GetInvoiceRatesResponse;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an invoice, such as totals, taxes, and estimates.
 */
@GrpcService
public class InvoiceCalculationEndpoint extends InvoiceCalculationServiceGrpc.InvoiceCalculationServiceImplBase {
    protected final InvoiceCalculationApplication application;

    public InvoiceCalculationEndpoint(InvoiceCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an invoice.
     */
    @Override
    public void calculateInvoice(CalculateInvoiceRequest request, StreamObserver<CalculateInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateInvoice(request));
    }

    /**
     * Estimates an invoice.
     */
    @Override
    public void estimateInvoice(EstimateInvoiceRequest request, StreamObserver<EstimateInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateInvoice(request));
    }

    /**
     * Compares an invoice.
     */
    @Override
    public void compareInvoiceOptions(CompareInvoiceOptionsRequest request, StreamObserver<CompareInvoiceOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareInvoiceOptions(request));
    }

    /**
     * Recalculates an invoice.
     */
    @Override
    public void recalculateInvoices(RecalculateInvoicesRequest request, StreamObserver<RecalculateInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateInvoices(request));
    }

    /**
     * Simulates an invoice.
     */
    @Override
    public void simulateInvoice(SimulateInvoiceRequest request, StreamObserver<SimulateInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateInvoice(request));
    }

    /**
     * Explains an invoice.
     */
    @Override
    public void explainInvoiceCalculation(ExplainInvoiceCalculationRequest request, StreamObserver<ExplainInvoiceCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceCalculation(request));
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void getInvoiceRates(GetInvoiceRatesRequest request, StreamObserver<GetInvoiceRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceRates(request));
    }
}
