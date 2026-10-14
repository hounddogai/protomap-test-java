package com.acme.finance.domains.billing;

import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.EstimatePaymentTermRequest;
import com.acme.billing.v1.EstimatePaymentTermResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a payment term, such as totals, taxes, and estimates.
 */
@GrpcService
public class PaymentTermCalculationEndpoint extends PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceImplBase {
    protected final PaymentTermCalculationApplication application;

    public PaymentTermCalculationEndpoint(PaymentTermCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a payment term.
     */
    @Override
    public void calculatePaymentTerm(CalculatePaymentTermRequest request, StreamObserver<CalculatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePaymentTerm(request));
    }

    /**
     * Estimates a payment term.
     */
    @Override
    public void estimatePaymentTerm(EstimatePaymentTermRequest request, StreamObserver<EstimatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePaymentTerm(request));
    }

    /**
     * Quotes a payment term.
     */
    @Override
    public void quotePaymentTerm(QuotePaymentTermRequest request, StreamObserver<QuotePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePaymentTerm(request));
    }

    /**
     * Compares a payment term.
     */
    @Override
    public void comparePaymentTermOptions(ComparePaymentTermOptionsRequest request, StreamObserver<ComparePaymentTermOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePaymentTermOptions(request));
    }

    /**
     * Recalculates a payment term.
     */
    @Override
    public void recalculatePaymentTerms(RecalculatePaymentTermsRequest request, StreamObserver<RecalculatePaymentTermsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculatePaymentTerms(request));
    }

    /**
     * Explains a payment term.
     */
    @Override
    public void explainPaymentTermCalculation(ExplainPaymentTermCalculationRequest request, StreamObserver<ExplainPaymentTermCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPaymentTermCalculation(request));
    }
}
