package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateDisposalRequest;
import com.acme.assets.v1.CalculateDisposalResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.ExplainDisposalCalculationRequest;
import com.acme.assets.v1.ExplainDisposalCalculationResponse;
import com.acme.assets.v1.GetDisposalRatesRequest;
import com.acme.assets.v1.GetDisposalRatesResponse;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.assets.v1.RecalculateDisposalsRequest;
import com.acme.assets.v1.RecalculateDisposalsResponse;
import com.acme.assets.v1.SimulateDisposalRequest;
import com.acme.assets.v1.SimulateDisposalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a disposal, such as totals, taxes, and estimates.
 */
@GrpcService
public class DisposalCalculationEndpoint extends DisposalCalculationServiceGrpc.DisposalCalculationServiceImplBase {
    protected final DisposalCalculationApplication application;

    public DisposalCalculationEndpoint(DisposalCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a disposal.
     */
    @Override
    public void calculateDisposal(CalculateDisposalRequest request, StreamObserver<CalculateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateDisposal(request));
    }

    /**
     * Estimates a disposal.
     */
    @Override
    public void estimateDisposal(EstimateDisposalRequest request, StreamObserver<EstimateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateDisposal(request));
    }

    /**
     * Quotes a disposal.
     */
    @Override
    public void quoteDisposal(QuoteDisposalRequest request, StreamObserver<QuoteDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteDisposal(request));
    }

    /**
     * Compares a disposal.
     */
    @Override
    public void compareDisposalOptions(CompareDisposalOptionsRequest request, StreamObserver<CompareDisposalOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareDisposalOptions(request));
    }

    /**
     * Recalculates a disposal.
     */
    @Override
    public void recalculateDisposals(RecalculateDisposalsRequest request, StreamObserver<RecalculateDisposalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateDisposals(request));
    }

    /**
     * Simulates a disposal.
     */
    @Override
    public void simulateDisposal(SimulateDisposalRequest request, StreamObserver<SimulateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateDisposal(request));
    }

    /**
     * Explains a disposal.
     */
    @Override
    public void explainDisposalCalculation(ExplainDisposalCalculationRequest request, StreamObserver<ExplainDisposalCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDisposalCalculation(request));
    }

    /**
     * Returns a disposal.
     */
    @Override
    public void getDisposalRates(GetDisposalRatesRequest request, StreamObserver<GetDisposalRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDisposalRates(request));
    }
}
