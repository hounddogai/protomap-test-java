package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.GetPurchaseOrderRatesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRatesResponse;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a purchase order, such as totals, taxes, and estimates.
 */
@GrpcService
public class PurchaseOrderCalculationEndpoint extends PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceImplBase {
    protected final PurchaseOrderCalculationApplication application;

    public PurchaseOrderCalculationEndpoint(PurchaseOrderCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a purchase order.
     */
    @Override
    public void calculatePurchaseOrder(CalculatePurchaseOrderRequest request, StreamObserver<CalculatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePurchaseOrder(request));
    }

    /**
     * Estimates a purchase order.
     */
    @Override
    public void estimatePurchaseOrder(EstimatePurchaseOrderRequest request, StreamObserver<EstimatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePurchaseOrder(request));
    }

    /**
     * Quotes a purchase order.
     */
    @Override
    public void quotePurchaseOrder(QuotePurchaseOrderRequest request, StreamObserver<QuotePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePurchaseOrder(request));
    }

    /**
     * Compares a purchase order.
     */
    @Override
    public void comparePurchaseOrderOptions(ComparePurchaseOrderOptionsRequest request, StreamObserver<ComparePurchaseOrderOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePurchaseOrderOptions(request));
    }

    /**
     * Simulates a purchase order.
     */
    @Override
    public void simulatePurchaseOrder(SimulatePurchaseOrderRequest request, StreamObserver<SimulatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulatePurchaseOrder(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void getPurchaseOrderRates(GetPurchaseOrderRatesRequest request, StreamObserver<GetPurchaseOrderRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseOrderRates(request));
    }
}
