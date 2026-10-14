package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CalculateMileageClaimRequest;
import com.acme.expenses.v1.CalculateMileageClaimResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.EstimateMileageClaimRequest;
import com.acme.expenses.v1.EstimateMileageClaimResponse;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.GetMileageClaimRatesRequest;
import com.acme.expenses.v1.GetMileageClaimRatesResponse;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.QuoteMileageClaimRequest;
import com.acme.expenses.v1.QuoteMileageClaimResponse;
import com.acme.expenses.v1.RecalculateMileageClaimsRequest;
import com.acme.expenses.v1.RecalculateMileageClaimsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a mileage claim, such as totals, taxes, and estimates.
 */
@GrpcService
public class MileageClaimCalculationEndpoint extends MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceImplBase {
    protected final MileageClaimCalculationApplication application;

    public MileageClaimCalculationEndpoint(MileageClaimCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a mileage claim.
     */
    @Override
    public void calculateMileageClaim(CalculateMileageClaimRequest request, StreamObserver<CalculateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateMileageClaim(request));
    }

    /**
     * Estimates a mileage claim.
     */
    @Override
    public void estimateMileageClaim(EstimateMileageClaimRequest request, StreamObserver<EstimateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateMileageClaim(request));
    }

    /**
     * Quotes a mileage claim.
     */
    @Override
    public void quoteMileageClaim(QuoteMileageClaimRequest request, StreamObserver<QuoteMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteMileageClaim(request));
    }

    /**
     * Compares a mileage claim.
     */
    @Override
    public void compareMileageClaimOptions(CompareMileageClaimOptionsRequest request, StreamObserver<CompareMileageClaimOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareMileageClaimOptions(request));
    }

    /**
     * Recalculates a mileage claim.
     */
    @Override
    public void recalculateMileageClaims(RecalculateMileageClaimsRequest request, StreamObserver<RecalculateMileageClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateMileageClaims(request));
    }

    /**
     * Explains a mileage claim.
     */
    @Override
    public void explainMileageClaimCalculation(ExplainMileageClaimCalculationRequest request, StreamObserver<ExplainMileageClaimCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainMileageClaimCalculation(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void getMileageClaimRates(GetMileageClaimRatesRequest request, StreamObserver<GetMileageClaimRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMileageClaimRates(request));
    }
}
