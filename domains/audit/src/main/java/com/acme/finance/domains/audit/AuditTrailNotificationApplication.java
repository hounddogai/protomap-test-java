package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.CancelAuditTrailExportRequest;
import com.acme.audit.v1.CancelAuditTrailExportResponse;
import com.acme.audit.v1.GetAuditTrailPreferencesRequest;
import com.acme.audit.v1.GetAuditTrailPreferencesResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ListAuditTrailSubscriptionsRequest;
import com.acme.audit.v1.ListAuditTrailSubscriptionsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow an audit trail what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class AuditTrailNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailNotificationApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final AuditRepository repository;

    public AuditTrailNotificationApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            AuditRepository repository) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.repository = repository;
    }

    /**
     * Subscribes an audit trail.
     */
    public SubscribeAuditTrailUpdatesResponse subscribeAuditTrailUpdates(SubscribeAuditTrailUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeAuditTrailUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "SubscribeAuditTrailUpdates");

        var outbound0 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input0 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeAuditTrailUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribeAuditTrailUpdatesResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }

    /**
     * Unsubscribes an audit trail.
     */
    public UnsubscribeAuditTrailUpdatesResponse unsubscribeAuditTrailUpdates(UnsubscribeAuditTrailUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeAuditTrailUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "UnsubscribeAuditTrailUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeAuditTrailUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribeAuditTrailUpdatesResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }

    /**
     * Notifys an audit trail.
     */
    public NotifyAuditTrailWatchersResponse notifyAuditTrailWatchers(NotifyAuditTrailWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyAuditTrailWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "NotifyAuditTrailWatchers");

        var outbound0 = CancelAuditTrailExportRequest.newBuilder()
                .setAuditTrailId(request.getAuditTrailId())
                .setContext(request.getContext())
                .setDetails(CancelAuditTrailExportRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelAuditTrailExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyAuditTrailWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyAuditTrailWatchersResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setAuditTrailId(request.getAuditTrailId())
                        .setStatus(reply0.getAuditTrail().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists audit trails a page at a time.
     */
    public ListAuditTrailSubscriptionsResponse listAuditTrailSubscriptions(ListAuditTrailSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditTrailSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "ListAuditTrailSubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditTrailSubscriptions", request.getPage().getPageToken());
        return ListAuditTrailSubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends an audit trail.
     */
    public SendAuditTrailTestNoticeResponse sendAuditTrailTestNotice(SendAuditTrailTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendAuditTrailTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "SendAuditTrailTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendAuditTrailTestNotice", request.getDetails().getRecipient().getEmail());
        return SendAuditTrailTestNoticeResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }

    /**
     * Returns an audit trail.
     */
    public GetAuditTrailPreferencesResponse getAuditTrailPreferences(GetAuditTrailPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditTrailPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "GetAuditTrailPreferences");
        return GetAuditTrailPreferencesResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of audit trails as they happen.
     */
    public StreamAuditTrailNoticesResponse streamAuditTrailNotices(StreamAuditTrailNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAuditTrailNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getContext().getTraceId(), "StreamAuditTrailNotices");

        var outbound0 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditTrailExportService");
        var serviceName0 = "acme.audit.v1" + "." + "AuditTrailExportService";
        var methodName0 = "Lis" + "tAuditTrailExports";
        var descriptor0 = MethodDescriptor.<ListAuditTrailExportsRequest, ListAuditTrailExportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ListAuditTrailExportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListAuditTrailExportsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAuditTrailNotices", request.getFilter().getQuery());
        return StreamAuditTrailNoticesResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
