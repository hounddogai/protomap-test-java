package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.BatchGetComplianceCasesRequest;
import com.acme.compliance.v1.BatchGetComplianceCasesResponse;
import com.acme.compliance.v1.CloneComplianceCaseRequest;
import com.acme.compliance.v1.CloneComplianceCaseResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.CreateComplianceCaseRequest;
import com.acme.compliance.v1.CreateComplianceCaseResponse;
import com.acme.compliance.v1.DeleteComplianceCaseRequest;
import com.acme.compliance.v1.DeleteComplianceCaseResponse;
import com.acme.compliance.v1.GetComplianceCaseRequest;
import com.acme.compliance.v1.GetComplianceCaseResponse;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.compliance.v1.SearchComplianceCasesRequest;
import com.acme.compliance.v1.SearchComplianceCasesResponse;
import com.acme.compliance.v1.UpdateComplianceCaseRequest;
import com.acme.compliance.v1.UpdateComplianceCaseResponse;
import com.acme.compliance.v1.WatchComplianceCasesRequest;
import com.acme.compliance.v1.WatchComplianceCasesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Manages compliance cases: creates, reads, changes, and lists them.
 */
public abstract class AbstractComplianceCaseEndpoint extends ComplianceCaseServiceGrpc.ComplianceCaseServiceImplBase {
    protected final ComplianceCaseApplication application;

    protected AbstractComplianceCaseEndpoint(ComplianceCaseApplication application) {
        this.application = application;
    }

    /**
     * Creates a compliance case.
     */
    @Override
    public void createComplianceCase(CreateComplianceCaseRequest request, StreamObserver<CreateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.createComplianceCase(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void getComplianceCase(GetComplianceCaseRequest request, StreamObserver<GetComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.getComplianceCase(request));
    }

    /**
     * Changes a compliance case.
     */
    @Override
    public void updateComplianceCase(UpdateComplianceCaseRequest request, StreamObserver<UpdateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateComplianceCase(request));
    }

    /**
     * Deletes a compliance case.
     */
    @Override
    public void deleteComplianceCase(DeleteComplianceCaseRequest request, StreamObserver<DeleteComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteComplianceCase(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void listComplianceCases(ListComplianceCasesRequest request, StreamObserver<ListComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listComplianceCases(request));
    }

    /**
     * Returns several compliance cases at once.
     */
    @Override
    public void batchGetComplianceCases(BatchGetComplianceCasesRequest request, StreamObserver<BatchGetComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetComplianceCases(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void searchComplianceCases(SearchComplianceCasesRequest request, StreamObserver<SearchComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchComplianceCases(request));
    }

    /**
     * Streams the changes of compliance cases as they happen.
     */
    @Override
    public void watchComplianceCases(WatchComplianceCasesRequest request, StreamObserver<WatchComplianceCasesResponse> observer) {
        try {
            var snapshot = application.watchComplianceCases(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of compliance cases and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateComplianceCasesRequest> batchCreateComplianceCases(StreamObserver<BatchCreateComplianceCasesResponse> observer) {
        return new StreamObserver<BatchCreateComplianceCasesRequest>() {
            private boolean finished;
            private final BatchCreateComplianceCasesResponse.Builder summary = BatchCreateComplianceCasesResponse.newBuilder();

            @Override
            public void onNext(BatchCreateComplianceCasesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateComplianceCases(request);
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
     * Creates a compliance case.
     */
    @Override
    public void cloneComplianceCase(CloneComplianceCaseRequest request, StreamObserver<CloneComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneComplianceCase(request));
    }
}
