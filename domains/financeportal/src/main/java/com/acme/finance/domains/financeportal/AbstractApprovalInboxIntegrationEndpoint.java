package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.IngestApprovalInboxEventsRequest;
import com.acme.financeportal.v1.IngestApprovalInboxEventsResponse;
import com.acme.financeportal.v1.ListApprovalInboxSourcesRequest;
import com.acme.financeportal.v1.ListApprovalInboxSourcesResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsRequest;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import io.grpc.stub.StreamObserver;

/**
 * Connects external systems that send approval inbox events.
 */
public abstract class AbstractApprovalInboxIntegrationEndpoint extends ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceImplBase {
    protected final ApprovalInboxIntegrationApplication application;

    protected AbstractApprovalInboxIntegrationEndpoint(ApprovalInboxIntegrationApplication application) {
        this.application = application;
    }

    /**
     * Connects an approval inbox.
     */
    @Override
    public void connectApprovalInboxSource(ConnectApprovalInboxSourceRequest request, StreamObserver<ConnectApprovalInboxSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.connectApprovalInboxSource(request));
    }

    /**
     * Disconnects an approval inbox.
     */
    @Override
    public void disconnectApprovalInboxSource(DisconnectApprovalInboxSourceRequest request, StreamObserver<DisconnectApprovalInboxSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.disconnectApprovalInboxSource(request));
    }

    /**
     * Takes a stream of approval inboxes and answers once at the end.
     */
    @Override
    public StreamObserver<IngestApprovalInboxEventsRequest> ingestApprovalInboxEvents(StreamObserver<IngestApprovalInboxEventsResponse> observer) {
        return new StreamObserver<IngestApprovalInboxEventsRequest>() {
            private boolean finished;
            private final IngestApprovalInboxEventsResponse.Builder summary = IngestApprovalInboxEventsResponse.newBuilder();

            @Override
            public void onNext(IngestApprovalInboxEventsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.ingestApprovalInboxEvents(request);
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
     * Replays an approval inbox.
     */
    @Override
    public void replayApprovalInboxEvents(ReplayApprovalInboxEventsRequest request, StreamObserver<ReplayApprovalInboxEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.replayApprovalInboxEvents(request));
    }

    /**
     * Exchanges changes of approval inboxes with a peer.
     */
    @Override
    public StreamObserver<RelayApprovalInboxMessagesRequest> relayApprovalInboxMessages(StreamObserver<RelayApprovalInboxMessagesResponse> observer) {
        return new StreamObserver<RelayApprovalInboxMessagesRequest>() {
            private boolean finished;
            private final RelayApprovalInboxMessagesResponse.Builder summary = RelayApprovalInboxMessagesResponse.newBuilder();

            @Override
            public void onNext(RelayApprovalInboxMessagesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.relayApprovalInboxMessages(request);
                    observer.onNext(response);
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
                observer.onCompleted();
            }
        };
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void listApprovalInboxSources(ListApprovalInboxSourcesRequest request, StreamObserver<ListApprovalInboxSourcesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listApprovalInboxSources(request));
    }

    /**
     * Tests an approval inbox.
     */
    @Override
    public void testApprovalInboxConnection(TestApprovalInboxConnectionRequest request, StreamObserver<TestApprovalInboxConnectionResponse> observer) {
        RpcSupport.unary(observer, () -> application.testApprovalInboxConnection(request));
    }
}
