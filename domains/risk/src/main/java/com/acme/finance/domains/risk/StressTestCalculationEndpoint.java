package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.EstimateStressTestRequest;
import com.acme.risk.v1.EstimateStressTestResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.GetStressTestRatesRequest;
import com.acme.risk.v1.GetStressTestRatesResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.SimulateStressTestRequest;
import com.acme.risk.v1.SimulateStressTestResponse;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a stress test, such as totals, taxes, and estimates.
 */
@GrpcService
public class StressTestCalculationEndpoint extends StressTestCalculationServiceGrpc.StressTestCalculationServiceImplBase {
    protected final StressTestCalculationApplication application;

    public StressTestCalculationEndpoint(StressTestCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a stress test.
     */
    @Override
    public void calculateStressTest(CalculateStressTestRequest request, StreamObserver<CalculateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateStressTest(request));
    }

    /**
     * Estimates a stress test.
     */
    @Override
    public void estimateStressTest(EstimateStressTestRequest request, StreamObserver<EstimateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateStressTest(request));
    }

    /**
     * Recalculates a stress test.
     */
    @Override
    public void recalculateStressTests(RecalculateStressTestsRequest request, StreamObserver<RecalculateStressTestsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateStressTests(request));
    }

    /**
     * Simulates a stress test.
     */
    @Override
    public void simulateStressTest(SimulateStressTestRequest request, StreamObserver<SimulateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateStressTest(request));
    }

    /**
     * Explains a stress test.
     */
    @Override
    public void explainStressTestCalculation(ExplainStressTestCalculationRequest request, StreamObserver<ExplainStressTestCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainStressTestCalculation(request));
    }

    /**
     * Returns a stress test.
     */
    @Override
    public void getStressTestRates(GetStressTestRatesRequest request, StreamObserver<GetStressTestRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getStressTestRates(request));
    }
}
