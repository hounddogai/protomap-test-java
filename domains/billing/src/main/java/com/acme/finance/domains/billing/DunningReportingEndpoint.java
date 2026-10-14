package com.acme.finance.domains.billing;

import com.acme.billing.v1.CountDunningTotalsRequest;
import com.acme.billing.v1.CountDunningTotalsResponse;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.FetchDunningReportRequest;
import com.acme.billing.v1.FetchDunningReportResponse;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.ListDunningReportsRequest;
import com.acme.billing.v1.ListDunningReportsResponse;
import com.acme.billing.v1.ScheduleDunningReportRequest;
import com.acme.billing.v1.ScheduleDunningReportResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.StreamDunningMetricsRequest;
import com.acme.billing.v1.StreamDunningMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about dunnings.
 */
@GrpcService
public class DunningReportingEndpoint extends DunningReportingServiceGrpc.DunningReportingServiceImplBase {
    protected final DunningReportingApplication application;

    public DunningReportingEndpoint(DunningReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a dunning.
     */
    @Override
    public void generateDunningReport(GenerateDunningReportRequest request, StreamObserver<GenerateDunningReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateDunningReport(request));
    }

    /**
     * Returns a dunning.
     */
    @Override
    public void fetchDunningReport(FetchDunningReportRequest request, StreamObserver<FetchDunningReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchDunningReport(request));
    }

    /**
     * Schedules a dunning.
     */
    @Override
    public void scheduleDunningReport(ScheduleDunningReportRequest request, StreamObserver<ScheduleDunningReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDunningReport(request));
    }

    /**
     * Streams the changes of dunnings as they happen.
     */
    @Override
    public void streamDunningMetrics(StreamDunningMetricsRequest request, StreamObserver<StreamDunningMetricsResponse> observer) {
        try {
            var snapshot = application.streamDunningMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists dunnings a page at a time.
     */
    @Override
    public void listDunningReports(ListDunningReportsRequest request, StreamObserver<ListDunningReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDunningReports(request));
    }

    /**
     * Shares a dunning.
     */
    @Override
    public void shareDunningReport(ShareDunningReportRequest request, StreamObserver<ShareDunningReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareDunningReport(request));
    }

    /**
     * Counts dunnings by status.
     */
    @Override
    public void countDunningTotals(CountDunningTotalsRequest request, StreamObserver<CountDunningTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countDunningTotals(request));
    }
}
