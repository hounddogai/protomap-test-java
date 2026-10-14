package com.acme.finance.domains.banking;

import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.GetMandateRatesRequest;
import com.acme.banking.v1.GetMandateRatesResponse;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a mandate, such as totals, taxes, and estimates.
 */
@GrpcService
public class MandateCalculationEndpoint extends MandateCalculationServiceGrpc.MandateCalculationServiceImplBase {
    protected final MandateCalculationApplication application;

    public MandateCalculationEndpoint(MandateCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a mandate.
     */
    @Override
    public void calculateMandate(CalculateMandateRequest request, StreamObserver<CalculateMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateMandate(request));
    }

    /**
     * Estimates a mandate.
     */
    @Override
    public void estimateMandate(EstimateMandateRequest request, StreamObserver<EstimateMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateMandate(request));
    }

    /**
     * Quotes a mandate.
     */
    @Override
    public void quoteMandate(QuoteMandateRequest request, StreamObserver<QuoteMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteMandate(request));
    }

    /**
     * Compares a mandate.
     */
    @Override
    public void compareMandateOptions(CompareMandateOptionsRequest request, StreamObserver<CompareMandateOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareMandateOptions(request));
    }

    /**
     * Recalculates a mandate.
     */
    @Override
    public void recalculateMandates(RecalculateMandatesRequest request, StreamObserver<RecalculateMandatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateMandates(request));
    }

    /**
     * Explains a mandate.
     */
    @Override
    public void explainMandateCalculation(ExplainMandateCalculationRequest request, StreamObserver<ExplainMandateCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainMandateCalculation(request));
    }

    /**
     * Returns a mandate.
     */
    @Override
    public void getMandateRates(GetMandateRatesRequest request, StreamObserver<GetMandateRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMandateRates(request));
    }
}
