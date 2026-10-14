package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CompareFxHedgeOptionsRequest;
import com.acme.treasury.v1.CompareFxHedgeOptionsResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a fx hedge, such as totals, taxes, and estimates.
 */
@GrpcService
public class FxHedgeCalculationEndpoint extends FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceImplBase {
    protected final FxHedgeCalculationApplication application;

    public FxHedgeCalculationEndpoint(FxHedgeCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a fx hedge.
     */
    @Override
    public void calculateFxHedge(CalculateFxHedgeRequest request, StreamObserver<CalculateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateFxHedge(request));
    }

    /**
     * Estimates a fx hedge.
     */
    @Override
    public void estimateFxHedge(EstimateFxHedgeRequest request, StreamObserver<EstimateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateFxHedge(request));
    }

    /**
     * Compares a fx hedge.
     */
    @Override
    public void compareFxHedgeOptions(CompareFxHedgeOptionsRequest request, StreamObserver<CompareFxHedgeOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareFxHedgeOptions(request));
    }

    /**
     * Recalculates a fx hedge.
     */
    @Override
    public void recalculateFxHedges(RecalculateFxHedgesRequest request, StreamObserver<RecalculateFxHedgesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateFxHedges(request));
    }

    /**
     * Simulates a fx hedge.
     */
    @Override
    public void simulateFxHedge(SimulateFxHedgeRequest request, StreamObserver<SimulateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateFxHedge(request));
    }

    /**
     * Explains a fx hedge.
     */
    @Override
    public void explainFxHedgeCalculation(ExplainFxHedgeCalculationRequest request, StreamObserver<ExplainFxHedgeCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainFxHedgeCalculation(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void getFxHedgeRates(GetFxHedgeRatesRequest request, StreamObserver<GetFxHedgeRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFxHedgeRates(request));
    }
}
