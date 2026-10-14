package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.DeleteTaxWithholdingReportRequest;
import com.acme.payroll.v1.DeleteTaxWithholdingReportResponse;
import com.acme.payroll.v1.FetchTaxWithholdingReportRequest;
import com.acme.payroll.v1.FetchTaxWithholdingReportResponse;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.ListTaxWithholdingReportsRequest;
import com.acme.payroll.v1.ListTaxWithholdingReportsResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about tax withholdings.
 */
@GrpcService
public class TaxWithholdingReportingEndpoint extends TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceImplBase {
    protected final TaxWithholdingReportingApplication application;

    public TaxWithholdingReportingEndpoint(TaxWithholdingReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a tax withholding.
     */
    @Override
    public void generateTaxWithholdingReport(GenerateTaxWithholdingReportRequest request, StreamObserver<GenerateTaxWithholdingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateTaxWithholdingReport(request));
    }

    /**
     * Returns a tax withholding.
     */
    @Override
    public void fetchTaxWithholdingReport(FetchTaxWithholdingReportRequest request, StreamObserver<FetchTaxWithholdingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchTaxWithholdingReport(request));
    }

    /**
     * Schedules a tax withholding.
     */
    @Override
    public void scheduleTaxWithholdingReport(ScheduleTaxWithholdingReportRequest request, StreamObserver<ScheduleTaxWithholdingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleTaxWithholdingReport(request));
    }

    /**
     * Lists tax withholdings a page at a time.
     */
    @Override
    public void listTaxWithholdingReports(ListTaxWithholdingReportsRequest request, StreamObserver<ListTaxWithholdingReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listTaxWithholdingReports(request));
    }

    /**
     * Shares a tax withholding.
     */
    @Override
    public void shareTaxWithholdingReport(ShareTaxWithholdingReportRequest request, StreamObserver<ShareTaxWithholdingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareTaxWithholdingReport(request));
    }

    /**
     * Deletes a tax withholding.
     */
    @Override
    public void deleteTaxWithholdingReport(DeleteTaxWithholdingReportRequest request, StreamObserver<DeleteTaxWithholdingReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteTaxWithholdingReport(request));
    }
}
