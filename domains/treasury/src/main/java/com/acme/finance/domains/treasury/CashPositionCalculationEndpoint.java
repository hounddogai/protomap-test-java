package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.SimulateCashPositionRequest;
import com.acme.treasury.v1.SimulateCashPositionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a cash position, such as totals, taxes, and estimates.
 */
@GrpcService
public class CashPositionCalculationEndpoint extends CashPositionCalculationServiceGrpc.CashPositionCalculationServiceImplBase {
    protected final CashPositionCalculationApplication application;

    public CashPositionCalculationEndpoint(CashPositionCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a cash position.
     */
    @Override
    public void calculateCashPosition(CalculateCashPositionRequest request, StreamObserver<CalculateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateCashPosition(request));
    }

    /**
     * Estimates a cash position.
     */
    @Override
    public void estimateCashPosition(EstimateCashPositionRequest request, StreamObserver<EstimateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateCashPosition(request));
    }

    /**
     * Quotes a cash position.
     */
    @Override
    public void quoteCashPosition(QuoteCashPositionRequest request, StreamObserver<QuoteCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteCashPosition(request));
    }

    /**
     * Compares a cash position.
     */
    @Override
    public void compareCashPositionOptions(CompareCashPositionOptionsRequest request, StreamObserver<CompareCashPositionOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareCashPositionOptions(request));
    }

    /**
     * Simulates a cash position.
     */
    @Override
    public void simulateCashPosition(SimulateCashPositionRequest request, StreamObserver<SimulateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateCashPosition(request));
    }

    /**
     * Explains a cash position.
     */
    @Override
    public void explainCashPositionCalculation(ExplainCashPositionCalculationRequest request, StreamObserver<ExplainCashPositionCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCashPositionCalculation(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void getCashPositionRates(GetCashPositionRatesRequest request, StreamObserver<GetCashPositionRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCashPositionRates(request));
    }
}
