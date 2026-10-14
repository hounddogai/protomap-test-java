package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CalculateControlRequest;
import com.acme.compliance.v1.CalculateControlResponse;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a control, such as totals, taxes, and estimates.
 */
@GrpcService
public class ControlCalculationEndpoint extends ControlCalculationServiceGrpc.ControlCalculationServiceImplBase {
    protected final ControlCalculationApplication application;

    public ControlCalculationEndpoint(ControlCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a control.
     */
    @Override
    public void calculateControl(CalculateControlRequest request, StreamObserver<CalculateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateControl(request));
    }

    /**
     * Estimates a control.
     */
    @Override
    public void estimateControl(EstimateControlRequest request, StreamObserver<EstimateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateControl(request));
    }

    /**
     * Quotes a control.
     */
    @Override
    public void quoteControl(QuoteControlRequest request, StreamObserver<QuoteControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteControl(request));
    }

    /**
     * Compares a control.
     */
    @Override
    public void compareControlOptions(CompareControlOptionsRequest request, StreamObserver<CompareControlOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareControlOptions(request));
    }

    /**
     * Recalculates a control.
     */
    @Override
    public void recalculateControls(RecalculateControlsRequest request, StreamObserver<RecalculateControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateControls(request));
    }

    /**
     * Simulates a control.
     */
    @Override
    public void simulateControl(SimulateControlRequest request, StreamObserver<SimulateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateControl(request));
    }

    /**
     * Explains a control.
     */
    @Override
    public void explainControlCalculation(ExplainControlCalculationRequest request, StreamObserver<ExplainControlCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainControlCalculation(request));
    }

    /**
     * Returns a control.
     */
    @Override
    public void getControlRates(GetControlRatesRequest request, StreamObserver<GetControlRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getControlRates(request));
    }
}
