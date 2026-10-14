package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DeleteAttestationReportRequest;
import com.acme.compliance.v1.DeleteAttestationReportResponse;
import com.acme.compliance.v1.FetchAttestationReportRequest;
import com.acme.compliance.v1.FetchAttestationReportResponse;
import com.acme.compliance.v1.GenerateAttestationReportRequest;
import com.acme.compliance.v1.GenerateAttestationReportResponse;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.StreamAttestationMetricsRequest;
import com.acme.compliance.v1.StreamAttestationMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about attestations.
 */
@GrpcService
public class AttestationReportingEndpoint extends AttestationReportingServiceGrpc.AttestationReportingServiceImplBase {
    protected final AttestationReportingApplication application;

    public AttestationReportingEndpoint(AttestationReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates an attestation.
     */
    @Override
    public void generateAttestationReport(GenerateAttestationReportRequest request, StreamObserver<GenerateAttestationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateAttestationReport(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void fetchAttestationReport(FetchAttestationReportRequest request, StreamObserver<FetchAttestationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchAttestationReport(request));
    }

    /**
     * Schedules an attestation.
     */
    @Override
    public void scheduleAttestationReport(ScheduleAttestationReportRequest request, StreamObserver<ScheduleAttestationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleAttestationReport(request));
    }

    /**
     * Streams the changes of attestations as they happen.
     */
    @Override
    public void streamAttestationMetrics(StreamAttestationMetricsRequest request, StreamObserver<StreamAttestationMetricsResponse> observer) {
        try {
            var snapshot = application.streamAttestationMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists attestations a page at a time.
     */
    @Override
    public void listAttestationReports(ListAttestationReportsRequest request, StreamObserver<ListAttestationReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAttestationReports(request));
    }

    /**
     * Shares an attestation.
     */
    @Override
    public void shareAttestationReport(ShareAttestationReportRequest request, StreamObserver<ShareAttestationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareAttestationReport(request));
    }

    /**
     * Counts attestations by status.
     */
    @Override
    public void countAttestationTotals(CountAttestationTotalsRequest request, StreamObserver<CountAttestationTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countAttestationTotals(request));
    }

    /**
     * Deletes an attestation.
     */
    @Override
    public void deleteAttestationReport(DeleteAttestationReportRequest request, StreamObserver<DeleteAttestationReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteAttestationReport(request));
    }
}
