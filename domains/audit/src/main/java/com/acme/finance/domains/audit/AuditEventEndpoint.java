package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.BatchGetAuditEventsRequest;
import com.acme.audit.v1.BatchGetAuditEventsResponse;
import com.acme.audit.v1.CloneAuditEventRequest;
import com.acme.audit.v1.CloneAuditEventResponse;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.DeleteAuditEventRequest;
import com.acme.audit.v1.DeleteAuditEventResponse;
import com.acme.audit.v1.GetAuditEventRequest;
import com.acme.audit.v1.GetAuditEventResponse;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.audit.v1.WatchAuditEventsRequest;
import com.acme.audit.v1.WatchAuditEventsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages audit events: creates, reads, changes, and lists them.
 */
@GrpcService
public class AuditEventEndpoint extends AuditEventServiceGrpc.AuditEventServiceImplBase {
    protected final AuditEventApplication application;

    public AuditEventEndpoint(AuditEventApplication application) {
        this.application = application;
    }

    /**
     * Creates an audit event.
     */
    @Override
    public void createAuditEvent(CreateAuditEventRequest request, StreamObserver<CreateAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.createAuditEvent(request));
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void getAuditEvent(GetAuditEventRequest request, StreamObserver<GetAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditEvent(request));
    }

    /**
     * Changes an audit event.
     */
    @Override
    public void updateAuditEvent(UpdateAuditEventRequest request, StreamObserver<UpdateAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateAuditEvent(request));
    }

    /**
     * Deletes an audit event.
     */
    @Override
    public void deleteAuditEvent(DeleteAuditEventRequest request, StreamObserver<DeleteAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteAuditEvent(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void listAuditEvents(ListAuditEventsRequest request, StreamObserver<ListAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditEvents(request));
    }

    /**
     * Returns several audit events at once.
     */
    @Override
    public void batchGetAuditEvents(BatchGetAuditEventsRequest request, StreamObserver<BatchGetAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetAuditEvents(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void searchAuditEvents(SearchAuditEventsRequest request, StreamObserver<SearchAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchAuditEvents(request));
    }

    /**
     * Streams the changes of audit events as they happen.
     */
    @Override
    public void watchAuditEvents(WatchAuditEventsRequest request, StreamObserver<WatchAuditEventsResponse> observer) {
        try {
            var snapshot = application.watchAuditEvents(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of audit events and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateAuditEventsRequest> batchCreateAuditEvents(StreamObserver<BatchCreateAuditEventsResponse> observer) {
        return new StreamObserver<BatchCreateAuditEventsRequest>() {
            private boolean finished;
            private final BatchCreateAuditEventsResponse.Builder summary = BatchCreateAuditEventsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateAuditEventsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateAuditEvents(request);
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
     * Creates an audit event.
     */
    @Override
    public void cloneAuditEvent(CloneAuditEventRequest request, StreamObserver<CloneAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneAuditEvent(request));
    }
}
