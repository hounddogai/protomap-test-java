package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.BatchGetExpenseReportsRequest;
import com.acme.expenses.v1.BatchGetExpenseReportsResponse;
import com.acme.expenses.v1.CloneExpenseReportRequest;
import com.acme.expenses.v1.CloneExpenseReportResponse;
import com.acme.expenses.v1.CreateExpenseReportRequest;
import com.acme.expenses.v1.CreateExpenseReportResponse;
import com.acme.expenses.v1.DeleteExpenseReportRequest;
import com.acme.expenses.v1.DeleteExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.expenses.v1.GetExpenseReportRequest;
import com.acme.expenses.v1.GetExpenseReportResponse;
import com.acme.expenses.v1.ListExpenseReportsRequest;
import com.acme.expenses.v1.ListExpenseReportsResponse;
import com.acme.expenses.v1.SearchExpenseReportsRequest;
import com.acme.expenses.v1.SearchExpenseReportsResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.expenses.v1.WatchExpenseReportsRequest;
import com.acme.expenses.v1.WatchExpenseReportsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Manages expense reports: creates, reads, changes, and lists them.
 */
public abstract class AbstractExpenseReportEndpoint extends ExpenseReportServiceGrpc.ExpenseReportServiceImplBase {
    protected final ExpenseReportApplication application;

    protected AbstractExpenseReportEndpoint(ExpenseReportApplication application) {
        this.application = application;
    }

    /**
     * Creates an expense report.
     */
    @Override
    public void createExpenseReport(CreateExpenseReportRequest request, StreamObserver<CreateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.createExpenseReport(request));
    }

    /**
     * Returns an expense report.
     */
    @Override
    public void getExpenseReport(GetExpenseReportRequest request, StreamObserver<GetExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpenseReport(request));
    }

    /**
     * Changes an expense report.
     */
    @Override
    public void updateExpenseReport(UpdateExpenseReportRequest request, StreamObserver<UpdateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateExpenseReport(request));
    }

    /**
     * Deletes an expense report.
     */
    @Override
    public void deleteExpenseReport(DeleteExpenseReportRequest request, StreamObserver<DeleteExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteExpenseReport(request));
    }

    /**
     * Lists expense reports a page at a time.
     */
    @Override
    public void listExpenseReports(ListExpenseReportsRequest request, StreamObserver<ListExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listExpenseReports(request));
    }

    /**
     * Returns several expense reports at once.
     */
    @Override
    public void batchGetExpenseReports(BatchGetExpenseReportsRequest request, StreamObserver<BatchGetExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetExpenseReports(request));
    }

    /**
     * Lists expense reports a page at a time.
     */
    @Override
    public void searchExpenseReports(SearchExpenseReportsRequest request, StreamObserver<SearchExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchExpenseReports(request));
    }

    /**
     * Streams the changes of expense reports as they happen.
     */
    @Override
    public void watchExpenseReports(WatchExpenseReportsRequest request, StreamObserver<WatchExpenseReportsResponse> observer) {
        try {
            var snapshot = application.watchExpenseReports(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of expense reports and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateExpenseReportsRequest> batchCreateExpenseReports(StreamObserver<BatchCreateExpenseReportsResponse> observer) {
        return new StreamObserver<BatchCreateExpenseReportsRequest>() {
            private boolean finished;
            private final BatchCreateExpenseReportsResponse.Builder summary = BatchCreateExpenseReportsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateExpenseReportsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateExpenseReports(request);
                    summary.mergeFrom(response);
                } catch (RuntimeException failure) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!finished) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onCompleted() {
                if (finished) { return; }
                finished = true;
                observer.onNext(summary.build());
                observer.onCompleted();
            }
        };
    }

    /**
     * Creates an expense report.
     */
    @Override
    public void cloneExpenseReport(CloneExpenseReportRequest request, StreamObserver<CloneExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneExpenseReport(request));
    }
}
