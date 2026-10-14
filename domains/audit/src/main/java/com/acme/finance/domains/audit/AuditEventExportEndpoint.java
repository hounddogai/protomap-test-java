package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.CancelAuditEventExportRequest;
import com.acme.audit.v1.CancelAuditEventExportResponse;
import com.acme.audit.v1.GetAuditEventExportRequest;
import com.acme.audit.v1.GetAuditEventExportResponse;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.RetryAuditEventExportRequest;
import com.acme.audit.v1.RetryAuditEventExportResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Exports audit events to files and other systems.
 */
@GrpcService
public class AuditEventExportEndpoint extends AuditEventExportServiceGrpc.AuditEventExportServiceImplBase {
    protected final AuditEventExportApplication application;

    public AuditEventExportEndpoint(AuditEventExportApplication application) {
        this.application = application;
    }

    /**
     * Streams every matching audit event.
     */
    @Override
    public void streamAuditEventExport(StreamAuditEventExportRequest request, StreamObserver<StreamAuditEventExportResponse> observer) {
        try {
            var snapshot = application.streamAuditEventExport(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void getAuditEventExport(GetAuditEventExportRequest request, StreamObserver<GetAuditEventExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditEventExport(request));
    }

    /**
     * Cancels an audit event.
     */
    @Override
    public void cancelAuditEventExport(CancelAuditEventExportRequest request, StreamObserver<CancelAuditEventExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAuditEventExport(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void listAuditEventExports(ListAuditEventExportsRequest request, StreamObserver<ListAuditEventExportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditEventExports(request));
    }

    /**
     * Schedules an audit event.
     */
    @Override
    public void scheduleAuditEventExport(ScheduleAuditEventExportRequest request, StreamObserver<ScheduleAuditEventExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleAuditEventExport(request));
    }

    /**
     * Retrys an audit event.
     */
    @Override
    public void retryAuditEventExport(RetryAuditEventExportRequest request, StreamObserver<RetryAuditEventExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryAuditEventExport(request));
    }

    /**
     * Shares an audit event.
     */
    @Override
    public void shareAuditEventExport(ShareAuditEventExportRequest request, StreamObserver<ShareAuditEventExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareAuditEventExport(request));
    }
}
