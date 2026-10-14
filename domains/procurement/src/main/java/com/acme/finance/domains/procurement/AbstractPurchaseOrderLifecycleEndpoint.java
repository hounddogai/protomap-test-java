package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ActivatePurchaseOrderRequest;
import com.acme.procurement.v1.ActivatePurchaseOrderResponse;
import com.acme.procurement.v1.ExpirePurchaseOrderRequest;
import com.acme.procurement.v1.ExpirePurchaseOrderResponse;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleRequest;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleResponse;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.ReinstatePurchaseOrderRequest;
import com.acme.procurement.v1.ReinstatePurchaseOrderResponse;
import com.acme.procurement.v1.RenewPurchaseOrderRequest;
import com.acme.procurement.v1.RenewPurchaseOrderResponse;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.SuspendPurchaseOrderRequest;
import com.acme.procurement.v1.SuspendPurchaseOrderResponse;
import com.acme.procurement.v1.TerminatePurchaseOrderRequest;
import com.acme.procurement.v1.TerminatePurchaseOrderResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves a purchase order between the states of its lifecycle.
 */
public abstract class AbstractPurchaseOrderLifecycleEndpoint extends PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceImplBase {
    protected final PurchaseOrderLifecycleApplication application;

    protected AbstractPurchaseOrderLifecycleEndpoint(PurchaseOrderLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a purchase order.
     */
    @Override
    public void activatePurchaseOrder(ActivatePurchaseOrderRequest request, StreamObserver<ActivatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.activatePurchaseOrder(request));
    }

    /**
     * Suspends a purchase order.
     */
    @Override
    public void suspendPurchaseOrder(SuspendPurchaseOrderRequest request, StreamObserver<SuspendPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendPurchaseOrder(request));
    }

    /**
     * Reinstates a purchase order.
     */
    @Override
    public void reinstatePurchaseOrder(ReinstatePurchaseOrderRequest request, StreamObserver<ReinstatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstatePurchaseOrder(request));
    }

    /**
     * Terminates a purchase order.
     */
    @Override
    public void terminatePurchaseOrder(TerminatePurchaseOrderRequest request, StreamObserver<TerminatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminatePurchaseOrder(request));
    }

    /**
     * Renews a purchase order.
     */
    @Override
    public void renewPurchaseOrder(RenewPurchaseOrderRequest request, StreamObserver<RenewPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewPurchaseOrder(request));
    }

    /**
     * Expires a purchase order.
     */
    @Override
    public void expirePurchaseOrder(ExpirePurchaseOrderRequest request, StreamObserver<ExpirePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.expirePurchaseOrder(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void getPurchaseOrderLifecycle(GetPurchaseOrderLifecycleRequest request, StreamObserver<GetPurchaseOrderLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseOrderLifecycle(request));
    }

    /**
     * Schedules a purchase order.
     */
    @Override
    public void schedulePurchaseOrderExpiry(SchedulePurchaseOrderExpiryRequest request, StreamObserver<SchedulePurchaseOrderExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePurchaseOrderExpiry(request));
    }
}
