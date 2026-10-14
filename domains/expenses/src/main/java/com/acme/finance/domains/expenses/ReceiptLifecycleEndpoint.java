package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ActivateReceiptRequest;
import com.acme.expenses.v1.ActivateReceiptResponse;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.GetReceiptLifecycleRequest;
import com.acme.expenses.v1.GetReceiptLifecycleResponse;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ReinstateReceiptRequest;
import com.acme.expenses.v1.ReinstateReceiptResponse;
import com.acme.expenses.v1.RenewReceiptRequest;
import com.acme.expenses.v1.RenewReceiptResponse;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.expenses.v1.SuspendReceiptRequest;
import com.acme.expenses.v1.SuspendReceiptResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a receipt between the states of its lifecycle.
 */
@GrpcService
public class ReceiptLifecycleEndpoint extends ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceImplBase {
    protected final ReceiptLifecycleApplication application;

    public ReceiptLifecycleEndpoint(ReceiptLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a receipt.
     */
    @Override
    public void activateReceipt(ActivateReceiptRequest request, StreamObserver<ActivateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateReceipt(request));
    }

    /**
     * Suspends a receipt.
     */
    @Override
    public void suspendReceipt(SuspendReceiptRequest request, StreamObserver<SuspendReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendReceipt(request));
    }

    /**
     * Reinstates a receipt.
     */
    @Override
    public void reinstateReceipt(ReinstateReceiptRequest request, StreamObserver<ReinstateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateReceipt(request));
    }

    /**
     * Terminates a receipt.
     */
    @Override
    public void terminateReceipt(TerminateReceiptRequest request, StreamObserver<TerminateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateReceipt(request));
    }

    /**
     * Renews a receipt.
     */
    @Override
    public void renewReceipt(RenewReceiptRequest request, StreamObserver<RenewReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewReceipt(request));
    }

    /**
     * Expires a receipt.
     */
    @Override
    public void expireReceipt(ExpireReceiptRequest request, StreamObserver<ExpireReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireReceipt(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void getReceiptLifecycle(GetReceiptLifecycleRequest request, StreamObserver<GetReceiptLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReceiptLifecycle(request));
    }

    /**
     * Schedules a receipt.
     */
    @Override
    public void scheduleReceiptExpiry(ScheduleReceiptExpiryRequest request, StreamObserver<ScheduleReceiptExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleReceiptExpiry(request));
    }
}
