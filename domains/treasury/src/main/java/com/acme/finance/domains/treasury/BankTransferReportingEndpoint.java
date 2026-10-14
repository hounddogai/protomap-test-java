package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CountBankTransferTotalsRequest;
import com.acme.treasury.v1.CountBankTransferTotalsResponse;
import com.acme.treasury.v1.DeleteBankTransferReportRequest;
import com.acme.treasury.v1.DeleteBankTransferReportResponse;
import com.acme.treasury.v1.FetchBankTransferReportRequest;
import com.acme.treasury.v1.FetchBankTransferReportResponse;
import com.acme.treasury.v1.GenerateBankTransferReportRequest;
import com.acme.treasury.v1.GenerateBankTransferReportResponse;
import com.acme.treasury.v1.ListBankTransferReportsRequest;
import com.acme.treasury.v1.ListBankTransferReportsResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.ShareBankTransferReportRequest;
import com.acme.treasury.v1.ShareBankTransferReportResponse;
import com.acme.treasury.v1.StreamBankTransferMetricsRequest;
import com.acme.treasury.v1.StreamBankTransferMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about bank transfers.
 */
@GrpcService
public class BankTransferReportingEndpoint extends BankTransferReportingServiceGrpc.BankTransferReportingServiceImplBase {
    protected final BankTransferReportingApplication application;

    public BankTransferReportingEndpoint(BankTransferReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a bank transfer.
     */
    @Override
    public void generateBankTransferReport(GenerateBankTransferReportRequest request, StreamObserver<GenerateBankTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateBankTransferReport(request));
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void fetchBankTransferReport(FetchBankTransferReportRequest request, StreamObserver<FetchBankTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchBankTransferReport(request));
    }

    /**
     * Schedules a bank transfer.
     */
    @Override
    public void scheduleBankTransferReport(ScheduleBankTransferReportRequest request, StreamObserver<ScheduleBankTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleBankTransferReport(request));
    }

    /**
     * Streams the changes of bank transfers as they happen.
     */
    @Override
    public void streamBankTransferMetrics(StreamBankTransferMetricsRequest request, StreamObserver<StreamBankTransferMetricsResponse> observer) {
        try {
            var snapshot = application.streamBankTransferMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void listBankTransferReports(ListBankTransferReportsRequest request, StreamObserver<ListBankTransferReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBankTransferReports(request));
    }

    /**
     * Shares a bank transfer.
     */
    @Override
    public void shareBankTransferReport(ShareBankTransferReportRequest request, StreamObserver<ShareBankTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareBankTransferReport(request));
    }

    /**
     * Counts bank transfers by status.
     */
    @Override
    public void countBankTransferTotals(CountBankTransferTotalsRequest request, StreamObserver<CountBankTransferTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBankTransferTotals(request));
    }

    /**
     * Deletes a bank transfer.
     */
    @Override
    public void deleteBankTransferReport(DeleteBankTransferReportRequest request, StreamObserver<DeleteBankTransferReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteBankTransferReport(request));
    }
}
