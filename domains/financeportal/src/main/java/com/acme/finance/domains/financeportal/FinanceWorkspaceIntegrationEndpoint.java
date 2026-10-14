package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesRequest;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionRequest;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Connects external systems that send finance workspace events.
 */
@GrpcService
public class FinanceWorkspaceIntegrationEndpoint extends FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceImplBase {
    protected final FinanceWorkspaceIntegrationApplication application;

    public FinanceWorkspaceIntegrationEndpoint(FinanceWorkspaceIntegrationApplication application) {
        this.application = application;
    }

    /**
     * Connects a finance workspace.
     */
    @Override
    public void connectFinanceWorkspaceSource(ConnectFinanceWorkspaceSourceRequest request, StreamObserver<ConnectFinanceWorkspaceSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.connectFinanceWorkspaceSource(request));
    }

    /**
     * Disconnects a finance workspace.
     */
    @Override
    public void disconnectFinanceWorkspaceSource(DisconnectFinanceWorkspaceSourceRequest request, StreamObserver<DisconnectFinanceWorkspaceSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.disconnectFinanceWorkspaceSource(request));
    }

    /**
     * Takes a stream of finance workspaces and answers once at the end.
     */
    @Override
    public StreamObserver<IngestFinanceWorkspaceEventsRequest> ingestFinanceWorkspaceEvents(StreamObserver<IngestFinanceWorkspaceEventsResponse> observer) {
        return new StreamObserver<IngestFinanceWorkspaceEventsRequest>() {
            private boolean finished;
            private final IngestFinanceWorkspaceEventsResponse.Builder summary = IngestFinanceWorkspaceEventsResponse.newBuilder();

            @Override
            public void onNext(IngestFinanceWorkspaceEventsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.ingestFinanceWorkspaceEvents(request);
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
     * Replays a finance workspace.
     */
    @Override
    public void replayFinanceWorkspaceEvents(ReplayFinanceWorkspaceEventsRequest request, StreamObserver<ReplayFinanceWorkspaceEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.replayFinanceWorkspaceEvents(request));
    }

    /**
     * Acknowledges a finance workspace.
     */
    @Override
    public void acknowledgeFinanceWorkspaceEvent(AcknowledgeFinanceWorkspaceEventRequest request, StreamObserver<AcknowledgeFinanceWorkspaceEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.acknowledgeFinanceWorkspaceEvent(request));
    }

    /**
     * Exchanges changes of finance workspaces with a peer.
     */
    @Override
    public StreamObserver<RelayFinanceWorkspaceMessagesRequest> relayFinanceWorkspaceMessages(StreamObserver<RelayFinanceWorkspaceMessagesResponse> observer) {
        return new StreamObserver<RelayFinanceWorkspaceMessagesRequest>() {
            private boolean finished;
            private final RelayFinanceWorkspaceMessagesResponse.Builder summary = RelayFinanceWorkspaceMessagesResponse.newBuilder();

            @Override
            public void onNext(RelayFinanceWorkspaceMessagesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.relayFinanceWorkspaceMessages(request);
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
     * Lists finance workspaces a page at a time.
     */
    @Override
    public void listFinanceWorkspaceSources(ListFinanceWorkspaceSourcesRequest request, StreamObserver<ListFinanceWorkspaceSourcesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFinanceWorkspaceSources(request));
    }

    /**
     * Tests a finance workspace.
     */
    @Override
    public void testFinanceWorkspaceConnection(TestFinanceWorkspaceConnectionRequest request, StreamObserver<TestFinanceWorkspaceConnectionResponse> observer) {
        RpcSupport.unary(observer, () -> application.testFinanceWorkspaceConnection(request));
    }
}
