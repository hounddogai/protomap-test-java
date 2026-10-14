package com.acme.finance.domains.billing;

import com.acme.billing.v1.CalculateDunningRequest;
import com.acme.billing.v1.CalculateDunningResponse;
import com.acme.billing.v1.CompareDunningOptionsRequest;
import com.acme.billing.v1.CompareDunningOptionsResponse;
import com.acme.billing.v1.DunningCalculationServiceGrpc;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExplainDunningCalculationRequest;
import com.acme.billing.v1.ExplainDunningCalculationResponse;
import com.acme.billing.v1.GetDunningRatesRequest;
import com.acme.billing.v1.GetDunningRatesResponse;
import com.acme.billing.v1.QuoteDunningRequest;
import com.acme.billing.v1.QuoteDunningResponse;
import com.acme.billing.v1.RecalculateDunningsRequest;
import com.acme.billing.v1.RecalculateDunningsResponse;
import com.acme.billing.v1.SimulateDunningRequest;
import com.acme.billing.v1.SimulateDunningResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a dunning, such as totals, taxes, and estimates.
 */
@GrpcService
public class DunningCalculationEndpoint extends DunningCalculationServiceGrpc.DunningCalculationServiceImplBase {
    protected final DunningCalculationApplication application;

    public DunningCalculationEndpoint(DunningCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a dunning.
     */
    @Override
    public void calculateDunning(CalculateDunningRequest request, StreamObserver<CalculateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateDunning(request));
    }

    /**
     * Estimates a dunning.
     */
    @Override
    public void estimateDunning(EstimateDunningRequest request, StreamObserver<EstimateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateDunning(request));
    }

    /**
     * Quotes a dunning.
     */
    @Override
    public void quoteDunning(QuoteDunningRequest request, StreamObserver<QuoteDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteDunning(request));
    }

    /**
     * Compares a dunning.
     */
    @Override
    public void compareDunningOptions(CompareDunningOptionsRequest request, StreamObserver<CompareDunningOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareDunningOptions(request));
    }

    /**
     * Recalculates a dunning.
     */
    @Override
    public void recalculateDunnings(RecalculateDunningsRequest request, StreamObserver<RecalculateDunningsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateDunnings(request));
    }

    /**
     * Simulates a dunning.
     */
    @Override
    public void simulateDunning(SimulateDunningRequest request, StreamObserver<SimulateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateDunning(request));
    }

    /**
     * Explains a dunning.
     */
    @Override
    public void explainDunningCalculation(ExplainDunningCalculationRequest request, StreamObserver<ExplainDunningCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDunningCalculation(request));
    }

    /**
     * Returns a dunning.
     */
    @Override
    public void getDunningRates(GetDunningRatesRequest request, StreamObserver<GetDunningRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDunningRates(request));
    }
}
