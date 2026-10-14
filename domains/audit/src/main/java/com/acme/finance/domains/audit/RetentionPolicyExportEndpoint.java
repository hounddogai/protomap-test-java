package com.acme.finance.domains.audit;

import com.acme.audit.v1.CancelRetentionPolicyExportRequest;
import com.acme.audit.v1.CancelRetentionPolicyExportResponse;
import com.acme.audit.v1.GetRetentionPolicyExportRequest;
import com.acme.audit.v1.GetRetentionPolicyExportResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.audit.v1.ScheduleRetentionPolicyExportRequest;
import com.acme.audit.v1.ScheduleRetentionPolicyExportResponse;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Exports retention policies to files and other systems.
 */
@GrpcService
public class RetentionPolicyExportEndpoint extends RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceImplBase {
    protected final RetentionPolicyExportApplication application;

    public RetentionPolicyExportEndpoint(RetentionPolicyExportApplication application) {
        this.application = application;
    }

    /**
     * Streams every matching retention policy.
     */
    @Override
    public void streamRetentionPolicyExport(StreamRetentionPolicyExportRequest request, StreamObserver<StreamRetentionPolicyExportResponse> observer) {
        try {
            var snapshot = application.streamRetentionPolicyExport(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Returns a retention policy.
     */
    @Override
    public void getRetentionPolicyExport(GetRetentionPolicyExportRequest request, StreamObserver<GetRetentionPolicyExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRetentionPolicyExport(request));
    }

    /**
     * Cancels a retention policy.
     */
    @Override
    public void cancelRetentionPolicyExport(CancelRetentionPolicyExportRequest request, StreamObserver<CancelRetentionPolicyExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelRetentionPolicyExport(request));
    }

    /**
     * Schedules a retention policy.
     */
    @Override
    public void scheduleRetentionPolicyExport(ScheduleRetentionPolicyExportRequest request, StreamObserver<ScheduleRetentionPolicyExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleRetentionPolicyExport(request));
    }

    /**
     * Retrys a retention policy.
     */
    @Override
    public void retryRetentionPolicyExport(RetryRetentionPolicyExportRequest request, StreamObserver<RetryRetentionPolicyExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryRetentionPolicyExport(request));
    }

    /**
     * Shares a retention policy.
     */
    @Override
    public void shareRetentionPolicyExport(ShareRetentionPolicyExportRequest request, StreamObserver<ShareRetentionPolicyExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareRetentionPolicyExport(request));
    }
}
