package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.BrowseExpenseReportsRequest;
import com.acme.expenses.v1.BrowseExpenseReportsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExportExpenseReportsRequest;
import com.acme.expenses.v1.ExportExpenseReportsResponse;
import com.acme.expenses.v1.FindExpenseReportsRequest;
import com.acme.expenses.v1.FindExpenseReportsResponse;
import com.acme.expenses.v1.LookupExpenseReportRequest;
import com.acme.expenses.v1.LookupExpenseReportResponse;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about expense reports, for screens and reports.
 */
@GrpcService
public class ExpenseReportQueryEndpoint extends ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceImplBase {
    protected final ExpenseReportQueryApplication application;

    public ExpenseReportQueryEndpoint(ExpenseReportQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an expense report.
     */
    @Override
    public void lookupExpenseReport(LookupExpenseReportRequest request, StreamObserver<LookupExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupExpenseReport(request));
    }

    /**
     * Lists expense reports a page at a time.
     */
    @Override
    public void browseExpenseReports(BrowseExpenseReportsRequest request, StreamObserver<BrowseExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseExpenseReports(request));
    }

    /**
     * Lists expense reports a page at a time.
     */
    @Override
    public void findExpenseReports(FindExpenseReportsRequest request, StreamObserver<FindExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findExpenseReports(request));
    }

    /**
     * Counts expense reports by status.
     */
    @Override
    public void countExpenseReports(CountExpenseReportsRequest request, StreamObserver<CountExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countExpenseReports(request));
    }

    /**
     * Returns several expense reports at once.
     */
    @Override
    public void resolveExpenseReports(ResolveExpenseReportsRequest request, StreamObserver<ResolveExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveExpenseReports(request));
    }

    /**
     * Streams every matching expense report.
     */
    @Override
    public void exportExpenseReports(ExportExpenseReportsRequest request, StreamObserver<ExportExpenseReportsResponse> observer) {
        try {
            var snapshot = application.exportExpenseReports(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
