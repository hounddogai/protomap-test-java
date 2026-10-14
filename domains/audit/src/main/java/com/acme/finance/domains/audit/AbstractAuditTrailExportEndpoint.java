package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.CancelAuditTrailExportRequest;
import com.acme.audit.v1.CancelAuditTrailExportResponse;
import com.acme.audit.v1.GetAuditTrailExportRequest;
import com.acme.audit.v1.GetAuditTrailExportResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Exports audit trails to files and other systems.
 */
public abstract class AbstractAuditTrailExportEndpoint extends AuditTrailExportServiceGrpc.AuditTrailExportServiceImplBase {
    protected final AuditTrailExportApplication application;

    protected AbstractAuditTrailExportEndpoint(AuditTrailExportApplication application) {
        this.application = application;
    }

    /**
     * Streams every matching audit trail.
     */
    @Override
    public void streamAuditTrailExport(StreamAuditTrailExportRequest request, StreamObserver<StreamAuditTrailExportResponse> observer) {
        try {
            var snapshot = application.streamAuditTrailExport(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void getAuditTrailExport(GetAuditTrailExportRequest request, StreamObserver<GetAuditTrailExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditTrailExport(request));
    }

    /**
     * Cancels an audit trail.
     */
    @Override
    public void cancelAuditTrailExport(CancelAuditTrailExportRequest request, StreamObserver<CancelAuditTrailExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAuditTrailExport(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void listAuditTrailExports(ListAuditTrailExportsRequest request, StreamObserver<ListAuditTrailExportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditTrailExports(request));
    }

    /**
     * Schedules an audit trail.
     */
    @Override
    public void scheduleAuditTrailExport(ScheduleAuditTrailExportRequest request, StreamObserver<ScheduleAuditTrailExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleAuditTrailExport(request));
    }

    /**
     * Retrys an audit trail.
     */
    @Override
    public void retryAuditTrailExport(RetryAuditTrailExportRequest request, StreamObserver<RetryAuditTrailExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryAuditTrailExport(request));
    }

    /**
     * Shares an audit trail.
     */
    @Override
    public void shareAuditTrailExport(ShareAuditTrailExportRequest request, StreamObserver<ShareAuditTrailExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareAuditTrailExport(request));
    }
}
