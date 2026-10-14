package com.acme.finance.domains.assets;

import com.acme.assets.v1.DeleteDepreciationReportRequest;
import com.acme.assets.v1.DeleteDepreciationReportResponse;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.FetchDepreciationReportRequest;
import com.acme.assets.v1.FetchDepreciationReportResponse;
import com.acme.assets.v1.GenerateDepreciationReportRequest;
import com.acme.assets.v1.GenerateDepreciationReportResponse;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about depreciations.
 */
@GrpcService
public class DepreciationReportingEndpoint extends DepreciationReportingServiceGrpc.DepreciationReportingServiceImplBase {
    protected final DepreciationReportingApplication application;

    public DepreciationReportingEndpoint(DepreciationReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a depreciation.
     */
    @Override
    public void generateDepreciationReport(GenerateDepreciationReportRequest request, StreamObserver<GenerateDepreciationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateDepreciationReport(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void fetchDepreciationReport(FetchDepreciationReportRequest request, StreamObserver<FetchDepreciationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchDepreciationReport(request));
    }

    /**
     * Streams the changes of depreciations as they happen.
     */
    @Override
    public void streamDepreciationMetrics(StreamDepreciationMetricsRequest request, StreamObserver<StreamDepreciationMetricsResponse> observer) {
        try {
            var snapshot = application.streamDepreciationMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void listDepreciationReports(ListDepreciationReportsRequest request, StreamObserver<ListDepreciationReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDepreciationReports(request));
    }

    /**
     * Shares a depreciation.
     */
    @Override
    public void shareDepreciationReport(ShareDepreciationReportRequest request, StreamObserver<ShareDepreciationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareDepreciationReport(request));
    }

    /**
     * Deletes a depreciation.
     */
    @Override
    public void deleteDepreciationReport(DeleteDepreciationReportRequest request, StreamObserver<DeleteDepreciationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteDepreciationReport(request));
    }
}
