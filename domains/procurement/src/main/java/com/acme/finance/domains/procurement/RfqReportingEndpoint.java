package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.FetchRfqReportRequest;
import com.acme.procurement.v1.FetchRfqReportResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about rfqs.
 */
@GrpcService
public class RfqReportingEndpoint extends RfqReportingServiceGrpc.RfqReportingServiceImplBase {
    protected final RfqReportingApplication application;

    public RfqReportingEndpoint(RfqReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a rfq.
     */
    @Override
    public void generateRfqReport(GenerateRfqReportRequest request, StreamObserver<GenerateRfqReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateRfqReport(request));
    }

    /**
     * Returns a rfq.
     */
    @Override
    public void fetchRfqReport(FetchRfqReportRequest request, StreamObserver<FetchRfqReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchRfqReport(request));
    }

    /**
     * Schedules a rfq.
     */
    @Override
    public void scheduleRfqReport(ScheduleRfqReportRequest request, StreamObserver<ScheduleRfqReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleRfqReport(request));
    }

    /**
     * Streams the changes of rfqs as they happen.
     */
    @Override
    public void streamRfqMetrics(StreamRfqMetricsRequest request, StreamObserver<StreamRfqMetricsResponse> observer) {
        try {
            var snapshot = application.streamRfqMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists rfqs a page at a time.
     */
    @Override
    public void listRfqReports(ListRfqReportsRequest request, StreamObserver<ListRfqReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRfqReports(request));
    }

    /**
     * Counts rfqs by status.
     */
    @Override
    public void countRfqTotals(CountRfqTotalsRequest request, StreamObserver<CountRfqTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countRfqTotals(request));
    }

    /**
     * Deletes a rfq.
     */
    @Override
    public void deleteRfqReport(DeleteRfqReportRequest request, StreamObserver<DeleteRfqReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteRfqReport(request));
    }
}
