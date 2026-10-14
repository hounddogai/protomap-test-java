package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CountFinanceWorkspacesRequest;
import com.acme.financeportal.v1.CountFinanceWorkspacesResponse;
import com.acme.financeportal.v1.ExportFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ExportFinanceWorkspacesResponse;
import com.acme.financeportal.v1.FinanceWorkspaceQueryServiceGrpc;
import com.acme.financeportal.v1.LookupFinanceWorkspaceRequest;
import com.acme.financeportal.v1.LookupFinanceWorkspaceResponse;
import com.acme.financeportal.v1.PeekFinanceWorkspaceRequest;
import com.acme.financeportal.v1.PeekFinanceWorkspaceResponse;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about finance workspaces, for screens and reports.
 */
@GrpcService
public class FinanceWorkspaceQueryEndpoint extends FinanceWorkspaceQueryServiceGrpc.FinanceWorkspaceQueryServiceImplBase {
    protected final FinanceWorkspaceQueryApplication application;

    public FinanceWorkspaceQueryEndpoint(FinanceWorkspaceQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a finance workspace.
     */
    @Override
    public void lookupFinanceWorkspace(LookupFinanceWorkspaceRequest request, StreamObserver<LookupFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupFinanceWorkspace(request));
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    @Override
    public void browseFinanceWorkspaces(BrowseFinanceWorkspacesRequest request, StreamObserver<BrowseFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseFinanceWorkspaces(request));
    }

    /**
     * Counts finance workspaces by status.
     */
    @Override
    public void countFinanceWorkspaces(CountFinanceWorkspacesRequest request, StreamObserver<CountFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countFinanceWorkspaces(request));
    }

    /**
     * Returns several finance workspaces at once.
     */
    @Override
    public void resolveFinanceWorkspaces(ResolveFinanceWorkspacesRequest request, StreamObserver<ResolveFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveFinanceWorkspaces(request));
    }

    /**
     * Streams every matching finance workspace.
     */
    @Override
    public void exportFinanceWorkspaces(ExportFinanceWorkspacesRequest request, StreamObserver<ExportFinanceWorkspacesResponse> observer) {
        try {
            var snapshot = application.exportFinanceWorkspaces(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a finance workspace.
     */
    @Override
    public void summarizeFinanceWorkspaces(SummarizeFinanceWorkspacesRequest request, StreamObserver<SummarizeFinanceWorkspacesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeFinanceWorkspaces(request));
    }

    /**
     * Returns a finance workspace.
     */
    @Override
    public void peekFinanceWorkspace(PeekFinanceWorkspaceRequest request, StreamObserver<PeekFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekFinanceWorkspace(request));
    }
}
