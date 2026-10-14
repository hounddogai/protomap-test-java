package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a rfq, such as totals, taxes, and estimates.
 */
public abstract class AbstractRfqCalculationEndpoint extends RfqCalculationServiceGrpc.RfqCalculationServiceImplBase {
    protected final RfqCalculationApplication application;

    protected AbstractRfqCalculationEndpoint(RfqCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a rfq.
     */
    @Override
    public void calculateRfq(CalculateRfqRequest request, StreamObserver<CalculateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateRfq(request));
    }

    /**
     * Estimates a rfq.
     */
    @Override
    public void estimateRfq(EstimateRfqRequest request, StreamObserver<EstimateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateRfq(request));
    }

    /**
     * Quotes a rfq.
     */
    @Override
    public void quoteRfq(QuoteRfqRequest request, StreamObserver<QuoteRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteRfq(request));
    }

    /**
     * Compares a rfq.
     */
    @Override
    public void compareRfqOptions(CompareRfqOptionsRequest request, StreamObserver<CompareRfqOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareRfqOptions(request));
    }

    /**
     * Simulates a rfq.
     */
    @Override
    public void simulateRfq(SimulateRfqRequest request, StreamObserver<SimulateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateRfq(request));
    }

    /**
     * Explains a rfq.
     */
    @Override
    public void explainRfqCalculation(ExplainRfqCalculationRequest request, StreamObserver<ExplainRfqCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRfqCalculation(request));
    }
}
