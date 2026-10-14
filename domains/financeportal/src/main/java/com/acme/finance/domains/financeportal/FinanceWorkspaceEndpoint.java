package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesResponse;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CloneFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CloneFinanceWorkspaceResponse;
import com.acme.financeportal.v1.CreateFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CreateFinanceWorkspaceResponse;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceRequest;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceResponse;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import com.acme.financeportal.v1.GetFinanceWorkspaceRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspacesResponse;
import com.acme.financeportal.v1.WatchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.WatchFinanceWorkspacesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages finance workspaces: creates, reads, changes, and lists them.
 */
@GrpcService
public class FinanceWorkspaceEndpoint extends FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceImplBase {
    protected final FinanceWorkspaceApplication application;

    public FinanceWorkspaceEndpoint(FinanceWorkspaceApplication application) {
        this.application = application;
    }

    /**
     * Creates a finance workspace.
     */
    @Override
    public void createFinanceWorkspace(CreateFinanceWorkspaceRequest request, StreamObserver<CreateFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.createFinanceWorkspace(request));
    }

    /**
     * Returns a finance workspace.
     */
    @Override
    public void getFinanceWorkspace(GetFinanceWorkspaceRequest request, StreamObserver<GetFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFinanceWorkspace(request));
    }

    /**
     * Deletes a finance workspace.
     */
    @Override
    public void deleteFinanceWorkspace(DeleteFinanceWorkspaceRequest request, StreamObserver<DeleteFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteFinanceWorkspace(request));
    }

    /**
     * Returns several finance workspaces at once.
     */
    @Override
    public void batchGetFinanceWorkspaces(BatchGetFinanceWorkspacesRequest request, StreamObserver<BatchGetFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetFinanceWorkspaces(request));
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    @Override
    public void searchFinanceWorkspaces(SearchFinanceWorkspacesRequest request, StreamObserver<SearchFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchFinanceWorkspaces(request));
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    @Override
    public void watchFinanceWorkspaces(WatchFinanceWorkspacesRequest request, StreamObserver<WatchFinanceWorkspacesResponse> observer) {
        try {
            var snapshot = application.watchFinanceWorkspaces(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of finance workspaces and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateFinanceWorkspacesRequest> batchCreateFinanceWorkspaces(StreamObserver<BatchCreateFinanceWorkspacesResponse> observer) {
        return new StreamObserver<BatchCreateFinanceWorkspacesRequest>() {
            private boolean finished;
            private final BatchCreateFinanceWorkspacesResponse.Builder summary = BatchCreateFinanceWorkspacesResponse.newBuilder();

            @Override
            public void onNext(BatchCreateFinanceWorkspacesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateFinanceWorkspaces(request);
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
     * Creates a finance workspace.
     */
    @Override
    public void cloneFinanceWorkspace(CloneFinanceWorkspaceRequest request, StreamObserver<CloneFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneFinanceWorkspace(request));
    }
}
