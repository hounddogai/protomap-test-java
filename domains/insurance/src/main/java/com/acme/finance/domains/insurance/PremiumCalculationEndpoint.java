package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CalculatePremiumRequest;
import com.acme.insurance.v1.CalculatePremiumResponse;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.EstimatePremiumRequest;
import com.acme.insurance.v1.EstimatePremiumResponse;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.GetPremiumRatesRequest;
import com.acme.insurance.v1.GetPremiumRatesResponse;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.QuotePremiumRequest;
import com.acme.insurance.v1.QuotePremiumResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.SimulatePremiumRequest;
import com.acme.insurance.v1.SimulatePremiumResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a premium, such as totals, taxes, and estimates.
 */
@GrpcService
public class PremiumCalculationEndpoint extends PremiumCalculationServiceGrpc.PremiumCalculationServiceImplBase {
    protected final PremiumCalculationApplication application;

    public PremiumCalculationEndpoint(PremiumCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a premium.
     */
    @Override
    public void calculatePremium(CalculatePremiumRequest request, StreamObserver<CalculatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePremium(request));
    }

    /**
     * Estimates a premium.
     */
    @Override
    public void estimatePremium(EstimatePremiumRequest request, StreamObserver<EstimatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePremium(request));
    }

    /**
     * Quotes a premium.
     */
    @Override
    public void quotePremium(QuotePremiumRequest request, StreamObserver<QuotePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePremium(request));
    }

    /**
     * Compares a premium.
     */
    @Override
    public void comparePremiumOptions(ComparePremiumOptionsRequest request, StreamObserver<ComparePremiumOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePremiumOptions(request));
    }

    /**
     * Recalculates a premium.
     */
    @Override
    public void recalculatePremiums(RecalculatePremiumsRequest request, StreamObserver<RecalculatePremiumsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculatePremiums(request));
    }

    /**
     * Simulates a premium.
     */
    @Override
    public void simulatePremium(SimulatePremiumRequest request, StreamObserver<SimulatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulatePremium(request));
    }

    /**
     * Explains a premium.
     */
    @Override
    public void explainPremiumCalculation(ExplainPremiumCalculationRequest request, StreamObserver<ExplainPremiumCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPremiumCalculation(request));
    }

    /**
     * Returns a premium.
     */
    @Override
    public void getPremiumRates(GetPremiumRatesRequest request, StreamObserver<GetPremiumRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPremiumRates(request));
    }
}
