package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ActivateRfqRequest;
import com.acme.procurement.v1.ActivateRfqResponse;
import com.acme.procurement.v1.ExpireRfqRequest;
import com.acme.procurement.v1.ExpireRfqResponse;
import com.acme.procurement.v1.GetRfqLifecycleRequest;
import com.acme.procurement.v1.GetRfqLifecycleResponse;
import com.acme.procurement.v1.ReinstateRfqRequest;
import com.acme.procurement.v1.ReinstateRfqResponse;
import com.acme.procurement.v1.RenewRfqRequest;
import com.acme.procurement.v1.RenewRfqResponse;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SuspendRfqRequest;
import com.acme.procurement.v1.SuspendRfqResponse;
import com.acme.procurement.v1.TerminateRfqRequest;
import com.acme.procurement.v1.TerminateRfqResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a rfq between the states of its lifecycle.
 */
@GrpcService
public class RfqLifecycleEndpoint extends RfqLifecycleServiceGrpc.RfqLifecycleServiceImplBase {
    protected final RfqLifecycleApplication application;

    public RfqLifecycleEndpoint(RfqLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a rfq.
     */
    @Override
    public void activateRfq(ActivateRfqRequest request, StreamObserver<ActivateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateRfq(request));
    }

    /**
     * Suspends a rfq.
     */
    @Override
    public void suspendRfq(SuspendRfqRequest request, StreamObserver<SuspendRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendRfq(request));
    }

    /**
     * Reinstates a rfq.
     */
    @Override
    public void reinstateRfq(ReinstateRfqRequest request, StreamObserver<ReinstateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateRfq(request));
    }

    /**
     * Terminates a rfq.
     */
    @Override
    public void terminateRfq(TerminateRfqRequest request, StreamObserver<TerminateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateRfq(request));
    }

    /**
     * Renews a rfq.
     */
    @Override
    public void renewRfq(RenewRfqRequest request, StreamObserver<RenewRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewRfq(request));
    }

    /**
     * Expires a rfq.
     */
    @Override
    public void expireRfq(ExpireRfqRequest request, StreamObserver<ExpireRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireRfq(request));
    }

    /**
     * Returns a rfq.
     */
    @Override
    public void getRfqLifecycle(GetRfqLifecycleRequest request, StreamObserver<GetRfqLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRfqLifecycle(request));
    }

    /**
     * Schedules a rfq.
     */
    @Override
    public void scheduleRfqExpiry(ScheduleRfqExpiryRequest request, StreamObserver<ScheduleRfqExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleRfqExpiry(request));
    }
}
