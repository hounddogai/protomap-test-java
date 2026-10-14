package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ActivateReconciliationRequest;
import com.acme.accounting.v1.ActivateReconciliationResponse;
import com.acme.accounting.v1.ExpireReconciliationRequest;
import com.acme.accounting.v1.ExpireReconciliationResponse;
import com.acme.accounting.v1.GetReconciliationLifecycleRequest;
import com.acme.accounting.v1.GetReconciliationLifecycleResponse;
import com.acme.accounting.v1.ReconciliationLifecycleServiceGrpc;
import com.acme.accounting.v1.RenewReconciliationRequest;
import com.acme.accounting.v1.RenewReconciliationResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.SuspendReconciliationRequest;
import com.acme.accounting.v1.SuspendReconciliationResponse;
import com.acme.accounting.v1.TerminateReconciliationRequest;
import com.acme.accounting.v1.TerminateReconciliationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a reconciliation between the states of its lifecycle.
 */
@GrpcService
public class ReconciliationLifecycleEndpoint extends ReconciliationLifecycleServiceGrpc.ReconciliationLifecycleServiceImplBase {
    protected final ReconciliationLifecycleApplication application;

    public ReconciliationLifecycleEndpoint(ReconciliationLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a reconciliation.
     */
    @Override
    public void activateReconciliation(ActivateReconciliationRequest request, StreamObserver<ActivateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateReconciliation(request));
    }

    /**
     * Suspends a reconciliation.
     */
    @Override
    public void suspendReconciliation(SuspendReconciliationRequest request, StreamObserver<SuspendReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendReconciliation(request));
    }

    /**
     * Terminates a reconciliation.
     */
    @Override
    public void terminateReconciliation(TerminateReconciliationRequest request, StreamObserver<TerminateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateReconciliation(request));
    }

    /**
     * Renews a reconciliation.
     */
    @Override
    public void renewReconciliation(RenewReconciliationRequest request, StreamObserver<RenewReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewReconciliation(request));
    }

    /**
     * Expires a reconciliation.
     */
    @Override
    public void expireReconciliation(ExpireReconciliationRequest request, StreamObserver<ExpireReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireReconciliation(request));
    }

    /**
     * Returns a reconciliation.
     */
    @Override
    public void getReconciliationLifecycle(GetReconciliationLifecycleRequest request, StreamObserver<GetReconciliationLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReconciliationLifecycle(request));
    }

    /**
     * Schedules a reconciliation.
     */
    @Override
    public void scheduleReconciliationExpiry(ScheduleReconciliationExpiryRequest request, StreamObserver<ScheduleReconciliationExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleReconciliationExpiry(request));
    }
}
