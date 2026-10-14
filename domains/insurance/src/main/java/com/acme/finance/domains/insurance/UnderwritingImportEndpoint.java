package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CancelUnderwritingImportRequest;
import com.acme.insurance.v1.CancelUnderwritingImportResponse;
import com.acme.insurance.v1.GetUnderwritingImportStatusRequest;
import com.acme.insurance.v1.GetUnderwritingImportStatusResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Takes underwritings in bulk from partners and legacy systems.
 */
@GrpcService
public class UnderwritingImportEndpoint extends UnderwritingImportServiceGrpc.UnderwritingImportServiceImplBase {
    protected final UnderwritingImportApplication application;

    public UnderwritingImportEndpoint(UnderwritingImportApplication application) {
        this.application = application;
    }

    /**
     * Takes a stream of underwritings and answers once at the end.
     */
    @Override
    public StreamObserver<ImportUnderwritingsRequest> importUnderwritings(StreamObserver<ImportUnderwritingsResponse> observer) {
        return new StreamObserver<ImportUnderwritingsRequest>() {
            private boolean finished;
            private final ImportUnderwritingsResponse.Builder summary = ImportUnderwritingsResponse.newBuilder();

            @Override
            public void onNext(ImportUnderwritingsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.importUnderwritings(request);
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
     * Returns an underwriting.
     */
    @Override
    public void getUnderwritingImportStatus(GetUnderwritingImportStatusRequest request, StreamObserver<GetUnderwritingImportStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getUnderwritingImportStatus(request));
    }

    /**
     * Cancels an underwriting.
     */
    @Override
    public void cancelUnderwritingImport(CancelUnderwritingImportRequest request, StreamObserver<CancelUnderwritingImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelUnderwritingImport(request));
    }

    /**
     * Validates an underwriting.
     */
    @Override
    public void validateUnderwritingImport(ValidateUnderwritingImportRequest request, StreamObserver<ValidateUnderwritingImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateUnderwritingImport(request));
    }

    /**
     * Lists underwritings a page at a time.
     */
    @Override
    public void listUnderwritingImports(ListUnderwritingImportsRequest request, StreamObserver<ListUnderwritingImportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listUnderwritingImports(request));
    }

    /**
     * Maps an underwriting.
     */
    @Override
    public void mapUnderwritingImportColumns(MapUnderwritingImportColumnsRequest request, StreamObserver<MapUnderwritingImportColumnsResponse> observer) {
        RpcSupport.unary(observer, () -> application.mapUnderwritingImportColumns(request));
    }
}
