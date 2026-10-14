package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.DeleteRepaymentReportRequest;
import com.acme.lending.v1.DeleteRepaymentReportResponse;
import com.acme.lending.v1.FetchRepaymentReportRequest;
import com.acme.lending.v1.FetchRepaymentReportResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentReportRequest;
import com.acme.lending.v1.ScheduleRepaymentReportResponse;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about repayments.
 */
@GrpcService
public class RepaymentReportingEndpoint extends RepaymentReportingServiceGrpc.RepaymentReportingServiceImplBase {
    protected final RepaymentReportingApplication application;

    public RepaymentReportingEndpoint(RepaymentReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a repayment.
     */
    @Override
    public void generateRepaymentReport(GenerateRepaymentReportRequest request, StreamObserver<GenerateRepaymentReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateRepaymentReport(request));
    }

    /**
     * Returns a repayment.
     */
    @Override
    public void fetchRepaymentReport(FetchRepaymentReportRequest request, StreamObserver<FetchRepaymentReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchRepaymentReport(request));
    }

    /**
     * Schedules a repayment.
     */
    @Override
    public void scheduleRepaymentReport(ScheduleRepaymentReportRequest request, StreamObserver<ScheduleRepaymentReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleRepaymentReport(request));
    }

    /**
     * Streams the changes of repayments as they happen.
     */
    @Override
    public void streamRepaymentMetrics(StreamRepaymentMetricsRequest request, StreamObserver<StreamRepaymentMetricsResponse> observer) {
        try {
            var snapshot = application.streamRepaymentMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Shares a repayment.
     */
    @Override
    public void shareRepaymentReport(ShareRepaymentReportRequest request, StreamObserver<ShareRepaymentReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareRepaymentReport(request));
    }

    /**
     * Counts repayments by status.
     */
    @Override
    public void countRepaymentTotals(CountRepaymentTotalsRequest request, StreamObserver<CountRepaymentTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countRepaymentTotals(request));
    }

    /**
     * Deletes a repayment.
     */
    @Override
    public void deleteRepaymentReport(DeleteRepaymentReportRequest request, StreamObserver<DeleteRepaymentReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteRepaymentReport(request));
    }
}
