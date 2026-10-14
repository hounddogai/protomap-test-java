package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DeleteAssetTransferReportRequest;
import com.acme.assets.v1.DeleteAssetTransferReportResponse;
import com.acme.assets.v1.FetchAssetTransferReportRequest;
import com.acme.assets.v1.FetchAssetTransferReportResponse;
import com.acme.assets.v1.GenerateAssetTransferReportRequest;
import com.acme.assets.v1.GenerateAssetTransferReportResponse;
import com.acme.assets.v1.ListAssetTransferReportsRequest;
import com.acme.assets.v1.ListAssetTransferReportsResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.ShareAssetTransferReportRequest;
import com.acme.assets.v1.ShareAssetTransferReportResponse;
import com.acme.assets.v1.StreamAssetTransferMetricsRequest;
import com.acme.assets.v1.StreamAssetTransferMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Builds and schedules reports about asset transfers.
 */
public abstract class AbstractAssetTransferReportingEndpoint extends AssetTransferReportingServiceGrpc.AssetTransferReportingServiceImplBase {
    protected final AssetTransferReportingApplication application;

    protected AbstractAssetTransferReportingEndpoint(AssetTransferReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates an asset transfer.
     */
    @Override
    public void generateAssetTransferReport(GenerateAssetTransferReportRequest request, StreamObserver<GenerateAssetTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateAssetTransferReport(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void fetchAssetTransferReport(FetchAssetTransferReportRequest request, StreamObserver<FetchAssetTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchAssetTransferReport(request));
    }

    /**
     * Schedules an asset transfer.
     */
    @Override
    public void scheduleAssetTransferReport(ScheduleAssetTransferReportRequest request, StreamObserver<ScheduleAssetTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleAssetTransferReport(request));
    }

    /**
     * Streams the changes of asset transfers as they happen.
     */
    @Override
    public void streamAssetTransferMetrics(StreamAssetTransferMetricsRequest request, StreamObserver<StreamAssetTransferMetricsResponse> observer) {
        try {
            var snapshot = application.streamAssetTransferMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists asset transfers a page at a time.
     */
    @Override
    public void listAssetTransferReports(ListAssetTransferReportsRequest request, StreamObserver<ListAssetTransferReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAssetTransferReports(request));
    }

    /**
     * Shares an asset transfer.
     */
    @Override
    public void shareAssetTransferReport(ShareAssetTransferReportRequest request, StreamObserver<ShareAssetTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareAssetTransferReport(request));
    }

    /**
     * Counts asset transfers by status.
     */
    @Override
    public void countAssetTransferTotals(CountAssetTransferTotalsRequest request, StreamObserver<CountAssetTransferTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countAssetTransferTotals(request));
    }

    /**
     * Deletes an asset transfer.
     */
    @Override
    public void deleteAssetTransferReport(DeleteAssetTransferReportRequest request, StreamObserver<DeleteAssetTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteAssetTransferReport(request));
    }
}
