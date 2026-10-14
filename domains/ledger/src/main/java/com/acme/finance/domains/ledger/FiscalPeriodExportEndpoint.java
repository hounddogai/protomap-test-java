package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CancelFiscalPeriodExportRequest;
import com.acme.ledger.v1.CancelFiscalPeriodExportResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.GetFiscalPeriodExportRequest;
import com.acme.ledger.v1.GetFiscalPeriodExportResponse;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Exports fiscal periods to files and other systems.
 */
@GrpcService
public class FiscalPeriodExportEndpoint extends FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceImplBase {
    protected final FiscalPeriodExportApplication application;

    public FiscalPeriodExportEndpoint(FiscalPeriodExportApplication application) {
        this.application = application;
    }

    /**
     * Streams every matching fiscal period.
     */
    @Override
    public void streamFiscalPeriodExport(StreamFiscalPeriodExportRequest request, StreamObserver<StreamFiscalPeriodExportResponse> observer) {
        try {
            var snapshot = application.streamFiscalPeriodExport(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Returns a fiscal period.
     */
    @Override
    public void getFiscalPeriodExport(GetFiscalPeriodExportRequest request, StreamObserver<GetFiscalPeriodExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFiscalPeriodExport(request));
    }

    /**
     * Cancels a fiscal period.
     */
    @Override
    public void cancelFiscalPeriodExport(CancelFiscalPeriodExportRequest request, StreamObserver<CancelFiscalPeriodExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelFiscalPeriodExport(request));
    }

    /**
     * Schedules a fiscal period.
     */
    @Override
    public void scheduleFiscalPeriodExport(ScheduleFiscalPeriodExportRequest request, StreamObserver<ScheduleFiscalPeriodExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleFiscalPeriodExport(request));
    }

    /**
     * Retrys a fiscal period.
     */
    @Override
    public void retryFiscalPeriodExport(RetryFiscalPeriodExportRequest request, StreamObserver<RetryFiscalPeriodExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryFiscalPeriodExport(request));
    }

    /**
     * Shares a fiscal period.
     */
    @Override
    public void shareFiscalPeriodExport(ShareFiscalPeriodExportRequest request, StreamObserver<ShareFiscalPeriodExportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareFiscalPeriodExport(request));
    }
}
