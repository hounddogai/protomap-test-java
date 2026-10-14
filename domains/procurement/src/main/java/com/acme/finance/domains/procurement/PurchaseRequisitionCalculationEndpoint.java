package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesResponse;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a purchase requisition, such as totals, taxes, and estimates.
 */
@GrpcService
public class PurchaseRequisitionCalculationEndpoint extends PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceImplBase {
    protected final PurchaseRequisitionCalculationApplication application;

    public PurchaseRequisitionCalculationEndpoint(PurchaseRequisitionCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a purchase requisition.
     */
    @Override
    public void calculatePurchaseRequisition(CalculatePurchaseRequisitionRequest request, StreamObserver<CalculatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePurchaseRequisition(request));
    }

    /**
     * Estimates a purchase requisition.
     */
    @Override
    public void estimatePurchaseRequisition(EstimatePurchaseRequisitionRequest request, StreamObserver<EstimatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePurchaseRequisition(request));
    }

    /**
     * Quotes a purchase requisition.
     */
    @Override
    public void quotePurchaseRequisition(QuotePurchaseRequisitionRequest request, StreamObserver<QuotePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePurchaseRequisition(request));
    }

    /**
     * Compares a purchase requisition.
     */
    @Override
    public void comparePurchaseRequisitionOptions(ComparePurchaseRequisitionOptionsRequest request, StreamObserver<ComparePurchaseRequisitionOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePurchaseRequisitionOptions(request));
    }

    /**
     * Recalculates a purchase requisition.
     */
    @Override
    public void recalculatePurchaseRequisitions(RecalculatePurchaseRequisitionsRequest request, StreamObserver<RecalculatePurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculatePurchaseRequisitions(request));
    }

    /**
     * Explains a purchase requisition.
     */
    @Override
    public void explainPurchaseRequisitionCalculation(ExplainPurchaseRequisitionCalculationRequest request, StreamObserver<ExplainPurchaseRequisitionCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPurchaseRequisitionCalculation(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void getPurchaseRequisitionRates(GetPurchaseRequisitionRatesRequest request, StreamObserver<GetPurchaseRequisitionRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseRequisitionRates(request));
    }
}
