package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ActivateStressTestRequest;
import com.acme.risk.v1.ActivateStressTestResponse;
import com.acme.risk.v1.ExpireStressTestRequest;
import com.acme.risk.v1.ExpireStressTestResponse;
import com.acme.risk.v1.ReinstateStressTestRequest;
import com.acme.risk.v1.ReinstateStressTestResponse;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.SuspendStressTestRequest;
import com.acme.risk.v1.SuspendStressTestResponse;
import com.acme.risk.v1.TerminateStressTestRequest;
import com.acme.risk.v1.TerminateStressTestResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a stress test between the states of its lifecycle.
 */
@GrpcService
public class StressTestLifecycleEndpoint extends StressTestLifecycleServiceGrpc.StressTestLifecycleServiceImplBase {
    protected final StressTestLifecycleApplication application;

    public StressTestLifecycleEndpoint(StressTestLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a stress test.
     */
    @Override
    public void activateStressTest(ActivateStressTestRequest request, StreamObserver<ActivateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateStressTest(request));
    }

    /**
     * Suspends a stress test.
     */
    @Override
    public void suspendStressTest(SuspendStressTestRequest request, StreamObserver<SuspendStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendStressTest(request));
    }

    /**
     * Reinstates a stress test.
     */
    @Override
    public void reinstateStressTest(ReinstateStressTestRequest request, StreamObserver<ReinstateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateStressTest(request));
    }

    /**
     * Terminates a stress test.
     */
    @Override
    public void terminateStressTest(TerminateStressTestRequest request, StreamObserver<TerminateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateStressTest(request));
    }

    /**
     * Expires a stress test.
     */
    @Override
    public void expireStressTest(ExpireStressTestRequest request, StreamObserver<ExpireStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireStressTest(request));
    }

    /**
     * Schedules a stress test.
     */
    @Override
    public void scheduleStressTestExpiry(ScheduleStressTestExpiryRequest request, StreamObserver<ScheduleStressTestExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleStressTestExpiry(request));
    }
}
