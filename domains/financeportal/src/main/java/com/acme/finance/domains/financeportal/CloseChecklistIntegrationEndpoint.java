package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.IngestCloseChecklistEventsRequest;
import com.acme.financeportal.v1.IngestCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ListCloseChecklistSourcesRequest;
import com.acme.financeportal.v1.ListCloseChecklistSourcesResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Connects external systems that send close checklist events.
 */
@GrpcService
public class CloseChecklistIntegrationEndpoint extends CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceImplBase {
    protected final CloseChecklistIntegrationApplication application;

    public CloseChecklistIntegrationEndpoint(CloseChecklistIntegrationApplication application) {
        this.application = application;
    }

    /**
     * Connects a close checklist.
     */
    @Override
    public void connectCloseChecklistSource(ConnectCloseChecklistSourceRequest request, StreamObserver<ConnectCloseChecklistSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.connectCloseChecklistSource(request));
    }

    /**
     * Disconnects a close checklist.
     */
    @Override
    public void disconnectCloseChecklistSource(DisconnectCloseChecklistSourceRequest request, StreamObserver<DisconnectCloseChecklistSourceResponse> observer) {
        RpcSupport.unary(observer, () -> application.disconnectCloseChecklistSource(request));
    }

    /**
     * Takes a stream of close checklists and answers once at the end.
     */
    @Override
    public StreamObserver<IngestCloseChecklistEventsRequest> ingestCloseChecklistEvents(StreamObserver<IngestCloseChecklistEventsResponse> observer) {
        return new StreamObserver<IngestCloseChecklistEventsRequest>() {
            private boolean finished;
            private final IngestCloseChecklistEventsResponse.Builder summary = IngestCloseChecklistEventsResponse.newBuilder();

            @Override
            public void onNext(IngestCloseChecklistEventsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.ingestCloseChecklistEvents(request);
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
     * Replays a close checklist.
     */
    @Override
    public void replayCloseChecklistEvents(ReplayCloseChecklistEventsRequest request, StreamObserver<ReplayCloseChecklistEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.replayCloseChecklistEvents(request));
    }

    /**
     * Acknowledges a close checklist.
     */
    @Override
    public void acknowledgeCloseChecklistEvent(AcknowledgeCloseChecklistEventRequest request, StreamObserver<AcknowledgeCloseChecklistEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.acknowledgeCloseChecklistEvent(request));
    }

    /**
     * Exchanges changes of close checklists with a peer.
     */
    @Override
    public StreamObserver<RelayCloseChecklistMessagesRequest> relayCloseChecklistMessages(StreamObserver<RelayCloseChecklistMessagesResponse> observer) {
        return new StreamObserver<RelayCloseChecklistMessagesRequest>() {
            private boolean finished;
            private final RelayCloseChecklistMessagesResponse.Builder summary = RelayCloseChecklistMessagesResponse.newBuilder();

            @Override
            public void onNext(RelayCloseChecklistMessagesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.relayCloseChecklistMessages(request);
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
     * Lists close checklists a page at a time.
     */
    @Override
    public void listCloseChecklistSources(ListCloseChecklistSourcesRequest request, StreamObserver<ListCloseChecklistSourcesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCloseChecklistSources(request));
    }

    /**
     * Tests a close checklist.
     */
    @Override
    public void testCloseChecklistConnection(TestCloseChecklistConnectionRequest request, StreamObserver<TestCloseChecklistConnectionResponse> observer) {
        RpcSupport.unary(observer, () -> application.testCloseChecklistConnection(request));
    }
}
