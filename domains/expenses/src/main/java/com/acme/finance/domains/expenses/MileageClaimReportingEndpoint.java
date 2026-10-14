package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.FetchMileageClaimReportRequest;
import com.acme.expenses.v1.FetchMileageClaimReportResponse;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListMileageClaimReportsRequest;
import com.acme.expenses.v1.ListMileageClaimReportsResponse;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.StreamMileageClaimMetricsRequest;
import com.acme.expenses.v1.StreamMileageClaimMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about mileage claims.
 */
@GrpcService
public class MileageClaimReportingEndpoint extends MileageClaimReportingServiceGrpc.MileageClaimReportingServiceImplBase {
    protected final MileageClaimReportingApplication application;

    public MileageClaimReportingEndpoint(MileageClaimReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a mileage claim.
     */
    @Override
    public void generateMileageClaimReport(GenerateMileageClaimReportRequest request, StreamObserver<GenerateMileageClaimReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateMileageClaimReport(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void fetchMileageClaimReport(FetchMileageClaimReportRequest request, StreamObserver<FetchMileageClaimReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchMileageClaimReport(request));
    }

    /**
     * Streams the changes of mileage claims as they happen.
     */
    @Override
    public void streamMileageClaimMetrics(StreamMileageClaimMetricsRequest request, StreamObserver<StreamMileageClaimMetricsResponse> observer) {
        try {
            var snapshot = application.streamMileageClaimMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists mileage claims a page at a time.
     */
    @Override
    public void listMileageClaimReports(ListMileageClaimReportsRequest request, StreamObserver<ListMileageClaimReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listMileageClaimReports(request));
    }

    /**
     * Shares a mileage claim.
     */
    @Override
    public void shareMileageClaimReport(ShareMileageClaimReportRequest request, StreamObserver<ShareMileageClaimReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareMileageClaimReport(request));
    }

    /**
     * Counts mileage claims by status.
     */
    @Override
    public void countMileageClaimTotals(CountMileageClaimTotalsRequest request, StreamObserver<CountMileageClaimTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countMileageClaimTotals(request));
    }
}
