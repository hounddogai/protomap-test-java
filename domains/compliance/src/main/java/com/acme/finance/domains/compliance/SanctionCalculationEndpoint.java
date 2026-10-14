package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExplainSanctionCalculationRequest;
import com.acme.compliance.v1.ExplainSanctionCalculationResponse;
import com.acme.compliance.v1.GetSanctionRatesRequest;
import com.acme.compliance.v1.GetSanctionRatesResponse;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SimulateSanctionRequest;
import com.acme.compliance.v1.SimulateSanctionResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a sanction, such as totals, taxes, and estimates.
 */
@GrpcService
public class SanctionCalculationEndpoint extends SanctionCalculationServiceGrpc.SanctionCalculationServiceImplBase {
    protected final SanctionCalculationApplication application;

    public SanctionCalculationEndpoint(SanctionCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a sanction.
     */
    @Override
    public void calculateSanction(CalculateSanctionRequest request, StreamObserver<CalculateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateSanction(request));
    }

    /**
     * Estimates a sanction.
     */
    @Override
    public void estimateSanction(EstimateSanctionRequest request, StreamObserver<EstimateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateSanction(request));
    }

    /**
     * Compares a sanction.
     */
    @Override
    public void compareSanctionOptions(CompareSanctionOptionsRequest request, StreamObserver<CompareSanctionOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareSanctionOptions(request));
    }

    /**
     * Simulates a sanction.
     */
    @Override
    public void simulateSanction(SimulateSanctionRequest request, StreamObserver<SimulateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateSanction(request));
    }

    /**
     * Explains a sanction.
     */
    @Override
    public void explainSanctionCalculation(ExplainSanctionCalculationRequest request, StreamObserver<ExplainSanctionCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainSanctionCalculation(request));
    }

    /**
     * Returns a sanction.
     */
    @Override
    public void getSanctionRates(GetSanctionRatesRequest request, StreamObserver<GetSanctionRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSanctionRates(request));
    }
}
