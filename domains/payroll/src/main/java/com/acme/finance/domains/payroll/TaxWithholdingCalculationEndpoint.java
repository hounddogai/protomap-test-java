package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationResponse;
import com.acme.payroll.v1.QuoteTaxWithholdingRequest;
import com.acme.payroll.v1.QuoteTaxWithholdingResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a tax withholding, such as totals, taxes, and estimates.
 */
@GrpcService
public class TaxWithholdingCalculationEndpoint extends TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceImplBase {
    protected final TaxWithholdingCalculationApplication application;

    public TaxWithholdingCalculationEndpoint(TaxWithholdingCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a tax withholding.
     */
    @Override
    public void calculateTaxWithholding(CalculateTaxWithholdingRequest request, StreamObserver<CalculateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateTaxWithholding(request));
    }

    /**
     * Estimates a tax withholding.
     */
    @Override
    public void estimateTaxWithholding(EstimateTaxWithholdingRequest request, StreamObserver<EstimateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateTaxWithholding(request));
    }

    /**
     * Quotes a tax withholding.
     */
    @Override
    public void quoteTaxWithholding(QuoteTaxWithholdingRequest request, StreamObserver<QuoteTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteTaxWithholding(request));
    }

    /**
     * Compares a tax withholding.
     */
    @Override
    public void compareTaxWithholdingOptions(CompareTaxWithholdingOptionsRequest request, StreamObserver<CompareTaxWithholdingOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareTaxWithholdingOptions(request));
    }

    /**
     * Recalculates a tax withholding.
     */
    @Override
    public void recalculateTaxWithholdings(RecalculateTaxWithholdingsRequest request, StreamObserver<RecalculateTaxWithholdingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateTaxWithholdings(request));
    }

    /**
     * Simulates a tax withholding.
     */
    @Override
    public void simulateTaxWithholding(SimulateTaxWithholdingRequest request, StreamObserver<SimulateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateTaxWithholding(request));
    }

    /**
     * Explains a tax withholding.
     */
    @Override
    public void explainTaxWithholdingCalculation(ExplainTaxWithholdingCalculationRequest request, StreamObserver<ExplainTaxWithholdingCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainTaxWithholdingCalculation(request));
    }
}
