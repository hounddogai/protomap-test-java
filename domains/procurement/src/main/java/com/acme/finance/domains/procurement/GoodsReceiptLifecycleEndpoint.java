package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ActivateGoodsReceiptRequest;
import com.acme.procurement.v1.ActivateGoodsReceiptResponse;
import com.acme.procurement.v1.ExpireGoodsReceiptRequest;
import com.acme.procurement.v1.ExpireGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleRequest;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleResponse;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.ReinstateGoodsReceiptRequest;
import com.acme.procurement.v1.ReinstateGoodsReceiptResponse;
import com.acme.procurement.v1.RenewGoodsReceiptRequest;
import com.acme.procurement.v1.RenewGoodsReceiptResponse;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryRequest;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryResponse;
import com.acme.procurement.v1.SuspendGoodsReceiptRequest;
import com.acme.procurement.v1.SuspendGoodsReceiptResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a goods receipt between the states of its lifecycle.
 */
@GrpcService
public class GoodsReceiptLifecycleEndpoint extends GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceImplBase {
    protected final GoodsReceiptLifecycleApplication application;

    public GoodsReceiptLifecycleEndpoint(GoodsReceiptLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a goods receipt.
     */
    @Override
    public void activateGoodsReceipt(ActivateGoodsReceiptRequest request, StreamObserver<ActivateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateGoodsReceipt(request));
    }

    /**
     * Suspends a goods receipt.
     */
    @Override
    public void suspendGoodsReceipt(SuspendGoodsReceiptRequest request, StreamObserver<SuspendGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendGoodsReceipt(request));
    }

    /**
     * Reinstates a goods receipt.
     */
    @Override
    public void reinstateGoodsReceipt(ReinstateGoodsReceiptRequest request, StreamObserver<ReinstateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateGoodsReceipt(request));
    }

    /**
     * Renews a goods receipt.
     */
    @Override
    public void renewGoodsReceipt(RenewGoodsReceiptRequest request, StreamObserver<RenewGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewGoodsReceipt(request));
    }

    /**
     * Expires a goods receipt.
     */
    @Override
    public void expireGoodsReceipt(ExpireGoodsReceiptRequest request, StreamObserver<ExpireGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireGoodsReceipt(request));
    }

    /**
     * Returns a goods receipt.
     */
    @Override
    public void getGoodsReceiptLifecycle(GetGoodsReceiptLifecycleRequest request, StreamObserver<GetGoodsReceiptLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getGoodsReceiptLifecycle(request));
    }

    /**
     * Schedules a goods receipt.
     */
    @Override
    public void scheduleGoodsReceiptExpiry(ScheduleGoodsReceiptExpiryRequest request, StreamObserver<ScheduleGoodsReceiptExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleGoodsReceiptExpiry(request));
    }
}
