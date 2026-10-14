package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageRequest;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageResponse;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewRequest;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewResponse;
import com.acme.financeportal.v1.FinanceWorkspaceGatewayServiceGrpc;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryResponse;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceRequest;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogResponse;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutRequest;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormRequest;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormResponse;
import com.acme.financeportal.v1.TrackFinanceWorkspaceRequest;
import com.acme.financeportal.v1.TrackFinanceWorkspaceResponse;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsRequest;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsResponse;
import io.grpc.stub.StreamObserver;

/**
 * The edge API that apps call for finance workspaces.
 */
public abstract class AbstractFinanceWorkspaceGatewayEndpoint extends FinanceWorkspaceGatewayServiceGrpc.FinanceWorkspaceGatewayServiceImplBase {
    protected final FinanceWorkspaceGatewayApplication application;

    protected AbstractFinanceWorkspaceGatewayEndpoint(FinanceWorkspaceGatewayApplication application) {
        this.application = application;
    }

    /**
     * Fetchs a finance workspace.
     */
    @Override
    public void fetchFinanceWorkspaceView(FetchFinanceWorkspaceViewRequest request, StreamObserver<FetchFinanceWorkspaceViewResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchFinanceWorkspaceView(request));
    }

    /**
     * Composes a finance workspace.
     */
    @Override
    public void composeFinanceWorkspacePage(ComposeFinanceWorkspacePageRequest request, StreamObserver<ComposeFinanceWorkspacePageResponse> observer) {
        RpcSupport.unary(observer, () -> application.composeFinanceWorkspacePage(request));
    }

    /**
     * Refreshs a finance workspace.
     */
    @Override
    public void refreshFinanceWorkspace(RefreshFinanceWorkspaceRequest request, StreamObserver<RefreshFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.refreshFinanceWorkspace(request));
    }

    /**
     * Submits a finance workspace.
     */
    @Override
    public void submitFinanceWorkspaceForm(SubmitFinanceWorkspaceFormRequest request, StreamObserver<SubmitFinanceWorkspaceFormResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitFinanceWorkspaceForm(request));
    }

    /**
     * Tracks a finance workspace.
     */
    @Override
    public void trackFinanceWorkspace(TrackFinanceWorkspaceRequest request, StreamObserver<TrackFinanceWorkspaceResponse> observer) {
        RpcSupport.unary(observer, () -> application.trackFinanceWorkspace(request));
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    @Override
    public void streamFinanceWorkspaceUpdates(StreamFinanceWorkspaceUpdatesRequest request, StreamObserver<StreamFinanceWorkspaceUpdatesResponse> observer) {
        try {
            var snapshot = application.streamFinanceWorkspaceUpdates(request);
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
    public StreamObserver<UploadFinanceWorkspaceAttachmentsRequest> uploadFinanceWorkspaceAttachments(StreamObserver<UploadFinanceWorkspaceAttachmentsResponse> observer) {
        return new StreamObserver<UploadFinanceWorkspaceAttachmentsRequest>() {
            private boolean finished;
            private final UploadFinanceWorkspaceAttachmentsResponse.Builder summary = UploadFinanceWorkspaceAttachmentsResponse.newBuilder();

            @Override
            public void onNext(UploadFinanceWorkspaceAttachmentsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.uploadFinanceWorkspaceAttachments(request);
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
     * Lists finance workspaces a page at a time.
     */
    @Override
    public void searchFinanceWorkspaceCatalog(SearchFinanceWorkspaceCatalogRequest request, StreamObserver<SearchFinanceWorkspaceCatalogResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchFinanceWorkspaceCatalog(request));
    }

    /**
     * Returns a finance workspace.
     */
    @Override
    public void getFinanceWorkspaceSummary(GetFinanceWorkspaceSummaryRequest request, StreamObserver<GetFinanceWorkspaceSummaryResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFinanceWorkspaceSummary(request));
    }

    /**
     * Starts a finance workspace.
     */
    @Override
    public void startFinanceWorkspaceCheckout(StartFinanceWorkspaceCheckoutRequest request, StreamObserver<StartFinanceWorkspaceCheckoutResponse> observer) {
        RpcSupport.unary(observer, () -> application.startFinanceWorkspaceCheckout(request));
    }
}
