package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CountUnderwritingTotalsRequest;
import com.acme.insurance.v1.CountUnderwritingTotalsResponse;
import com.acme.insurance.v1.DeleteUnderwritingReportRequest;
import com.acme.insurance.v1.DeleteUnderwritingReportResponse;
import com.acme.insurance.v1.FetchUnderwritingReportRequest;
import com.acme.insurance.v1.FetchUnderwritingReportResponse;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about underwritings.
 */
@GrpcService
public class UnderwritingReportingEndpoint extends UnderwritingReportingServiceGrpc.UnderwritingReportingServiceImplBase {
    protected final UnderwritingReportingApplication application;

    public UnderwritingReportingEndpoint(UnderwritingReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates an underwriting.
     */
    @Override
    public void generateUnderwritingReport(GenerateUnderwritingReportRequest request, StreamObserver<GenerateUnderwritingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateUnderwritingReport(request));
    }

    /**
     * Returns an underwriting.
     */
    @Override
    public void fetchUnderwritingReport(FetchUnderwritingReportRequest request, StreamObserver<FetchUnderwritingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchUnderwritingReport(request));
    }

    /**
     * Streams the changes of underwritings as they happen.
     */
    @Override
    public void streamUnderwritingMetrics(StreamUnderwritingMetricsRequest request, StreamObserver<StreamUnderwritingMetricsResponse> observer) {
        try {
            var snapshot = application.streamUnderwritingMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Shares an underwriting.
     */
    @Override
    public void shareUnderwritingReport(ShareUnderwritingReportRequest request, StreamObserver<ShareUnderwritingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareUnderwritingReport(request));
    }

    /**
     * Counts underwritings by status.
     */
    @Override
    public void countUnderwritingTotals(CountUnderwritingTotalsRequest request, StreamObserver<CountUnderwritingTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countUnderwritingTotals(request));
    }

    /**
     * Deletes an underwriting.
     */
    @Override
    public void deleteUnderwritingReport(DeleteUnderwritingReportRequest request, StreamObserver<DeleteUnderwritingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteUnderwritingReport(request));
    }
}
