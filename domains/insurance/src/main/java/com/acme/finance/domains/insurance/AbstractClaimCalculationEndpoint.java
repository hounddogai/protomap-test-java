package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.GetClaimRatesRequest;
import com.acme.insurance.v1.GetClaimRatesResponse;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a claim, such as totals, taxes, and estimates.
 */
public abstract class AbstractClaimCalculationEndpoint extends ClaimCalculationServiceGrpc.ClaimCalculationServiceImplBase {
    protected final ClaimCalculationApplication application;

    protected AbstractClaimCalculationEndpoint(ClaimCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a claim.
     */
    @Override
    public void calculateClaim(CalculateClaimRequest request, StreamObserver<CalculateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateClaim(request));
    }

    /**
     * Estimates a claim.
     */
    @Override
    public void estimateClaim(EstimateClaimRequest request, StreamObserver<EstimateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateClaim(request));
    }

    /**
     * Quotes a claim.
     */
    @Override
    public void quoteClaim(QuoteClaimRequest request, StreamObserver<QuoteClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteClaim(request));
    }

    /**
     * Compares a claim.
     */
    @Override
    public void compareClaimOptions(CompareClaimOptionsRequest request, StreamObserver<CompareClaimOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareClaimOptions(request));
    }

    /**
     * Recalculates a claim.
     */
    @Override
    public void recalculateClaims(RecalculateClaimsRequest request, StreamObserver<RecalculateClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateClaims(request));
    }

    /**
     * Simulates a claim.
     */
    @Override
    public void simulateClaim(SimulateClaimRequest request, StreamObserver<SimulateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateClaim(request));
    }

    /**
     * Explains a claim.
     */
    @Override
    public void explainClaimCalculation(ExplainClaimCalculationRequest request, StreamObserver<ExplainClaimCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainClaimCalculation(request));
    }

    /**
     * Returns a claim.
     */
    @Override
    public void getClaimRates(GetClaimRatesRequest request, StreamObserver<GetClaimRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getClaimRates(request));
    }
}
