package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.DeleteEInvoiceReportRequest;
import com.acme.invoicing.v1.DeleteEInvoiceReportResponse;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.FetchEInvoiceReportRequest;
import com.acme.invoicing.v1.FetchEInvoiceReportResponse;
import com.acme.invoicing.v1.GenerateEInvoiceReportRequest;
import com.acme.invoicing.v1.GenerateEInvoiceReportResponse;
import com.acme.invoicing.v1.ListEInvoiceReportsRequest;
import com.acme.invoicing.v1.ListEInvoiceReportsResponse;
import com.acme.invoicing.v1.ShareEInvoiceReportRequest;
import com.acme.invoicing.v1.ShareEInvoiceReportResponse;
import com.acme.invoicing.v1.StreamEInvoiceMetricsRequest;
import com.acme.invoicing.v1.StreamEInvoiceMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about e invoices.
 */
@GrpcService
public class EInvoiceReportingEndpoint extends EInvoiceReportingServiceGrpc.EInvoiceReportingServiceImplBase {
    protected final EInvoiceReportingApplication application;

    public EInvoiceReportingEndpoint(EInvoiceReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates an e invoice.
     */
    @Override
    public void generateEInvoiceReport(GenerateEInvoiceReportRequest request, StreamObserver<GenerateEInvoiceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateEInvoiceReport(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void fetchEInvoiceReport(FetchEInvoiceReportRequest request, StreamObserver<FetchEInvoiceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchEInvoiceReport(request));
    }

    /**
     * Streams the changes of e invoices as they happen.
     */
    @Override
    public void streamEInvoiceMetrics(StreamEInvoiceMetricsRequest request, StreamObserver<StreamEInvoiceMetricsResponse> observer) {
        try {
            var snapshot = application.streamEInvoiceMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists e invoices a page at a time.
     */
    @Override
    public void listEInvoiceReports(ListEInvoiceReportsRequest request, StreamObserver<ListEInvoiceReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listEInvoiceReports(request));
    }

    /**
     * Shares an e invoice.
     */
    @Override
    public void shareEInvoiceReport(ShareEInvoiceReportRequest request, StreamObserver<ShareEInvoiceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareEInvoiceReport(request));
    }

    /**
     * Counts e invoices by status.
     */
    @Override
    public void countEInvoiceTotals(CountEInvoiceTotalsRequest request, StreamObserver<CountEInvoiceTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countEInvoiceTotals(request));
    }

    /**
     * Deletes an e invoice.
     */
    @Override
    public void deleteEInvoiceReport(DeleteEInvoiceReportRequest request, StreamObserver<DeleteEInvoiceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteEInvoiceReport(request));
    }
}
