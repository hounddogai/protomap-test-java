package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.DeleteSanctionReportRequest;
import com.acme.compliance.v1.DeleteSanctionReportResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GenerateSanctionReportRequest;
import com.acme.compliance.v1.GenerateSanctionReportResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about sanctions.
 */
@GrpcService
public class SanctionReportingEndpoint extends SanctionReportingServiceGrpc.SanctionReportingServiceImplBase {
    protected final SanctionReportingApplication application;

    public SanctionReportingEndpoint(SanctionReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a sanction.
     */
    @Override
    public void generateSanctionReport(GenerateSanctionReportRequest request, StreamObserver<GenerateSanctionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateSanctionReport(request));
    }

    /**
     * Returns a sanction.
     */
    @Override
    public void fetchSanctionReport(FetchSanctionReportRequest request, StreamObserver<FetchSanctionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchSanctionReport(request));
    }

    /**
     * Schedules a sanction.
     */
    @Override
    public void scheduleSanctionReport(ScheduleSanctionReportRequest request, StreamObserver<ScheduleSanctionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleSanctionReport(request));
    }

    /**
     * Streams the changes of sanctions as they happen.
     */
    @Override
    public void streamSanctionMetrics(StreamSanctionMetricsRequest request, StreamObserver<StreamSanctionMetricsResponse> observer) {
        try {
            var snapshot = application.streamSanctionMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists sanctions a page at a time.
     */
    @Override
    public void listSanctionReports(ListSanctionReportsRequest request, StreamObserver<ListSanctionReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listSanctionReports(request));
    }

    /**
     * Counts sanctions by status.
     */
    @Override
    public void countSanctionTotals(CountSanctionTotalsRequest request, StreamObserver<CountSanctionTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countSanctionTotals(request));
    }

    /**
     * Deletes a sanction.
     */
    @Override
    public void deleteSanctionReport(DeleteSanctionReportRequest request, StreamObserver<DeleteSanctionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteSanctionReport(request));
    }
}
