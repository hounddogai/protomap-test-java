package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CancelVarianceImportRequest;
import com.acme.budgeting.v1.CancelVarianceImportResponse;
import com.acme.budgeting.v1.GetVarianceImportStatusRequest;
import com.acme.budgeting.v1.GetVarianceImportStatusResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.MapVarianceImportColumnsRequest;
import com.acme.budgeting.v1.MapVarianceImportColumnsResponse;
import com.acme.budgeting.v1.RetryVarianceImportRequest;
import com.acme.budgeting.v1.RetryVarianceImportResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Takes variances in bulk from partners and legacy systems.
 */
@GrpcService
public class VarianceImportEndpoint extends VarianceImportServiceGrpc.VarianceImportServiceImplBase {
    protected final VarianceImportApplication application;

    public VarianceImportEndpoint(VarianceImportApplication application) {
        this.application = application;
    }

    /**
     * Takes a stream of variances and answers once at the end.
     */
    @Override
    public StreamObserver<ImportVariancesRequest> importVariances(StreamObserver<ImportVariancesResponse> observer) {
        return new StreamObserver<ImportVariancesRequest>() {
            private boolean finished;
            private final ImportVariancesResponse.Builder summary = ImportVariancesResponse.newBuilder();

            @Override
            public void onNext(ImportVariancesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.importVariances(request);
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
     * Returns a variance.
     */
    @Override
    public void getVarianceImportStatus(GetVarianceImportStatusRequest request, StreamObserver<GetVarianceImportStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceImportStatus(request));
    }

    /**
     * Cancels a variance.
     */
    @Override
    public void cancelVarianceImport(CancelVarianceImportRequest request, StreamObserver<CancelVarianceImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelVarianceImport(request));
    }

    /**
     * Validates a variance.
     */
    @Override
    public void validateVarianceImport(ValidateVarianceImportRequest request, StreamObserver<ValidateVarianceImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateVarianceImport(request));
    }

    /**
     * Lists variances a page at a time.
     */
    @Override
    public void listVarianceImports(ListVarianceImportsRequest request, StreamObserver<ListVarianceImportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listVarianceImports(request));
    }

    /**
     * Retrys a variance.
     */
    @Override
    public void retryVarianceImport(RetryVarianceImportRequest request, StreamObserver<RetryVarianceImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryVarianceImport(request));
    }

    /**
     * Maps a variance.
     */
    @Override
    public void mapVarianceImportColumns(MapVarianceImportColumnsRequest request, StreamObserver<MapVarianceImportColumnsResponse> observer) {
        RpcSupport.unary(observer, () -> application.mapVarianceImportColumns(request));
    }
}
