package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DelegateFixedAssetApprovalRequest;
import com.acme.assets.v1.DelegateFixedAssetApprovalResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.EscalateFixedAssetRequest;
import com.acme.assets.v1.EscalateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetFixedAssetApprovalStatusRequest;
import com.acme.assets.v1.GetFixedAssetApprovalStatusResponse;
import com.acme.assets.v1.ListPendingFixedAssetsRequest;
import com.acme.assets.v1.ListPendingFixedAssetsResponse;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.assets.v1.RecallFixedAssetRequest;
import com.acme.assets.v1.RecallFixedAssetResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.fleet.v1.ListVehiclesRequest;
import com.acme.fleet.v1.ListVehiclesResponse;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.SuggestUsersRequest;
import com.acme.identity.v1.SuggestUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.LintNotificationsRequest;
import com.acme.notifications.v1.LintNotificationsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes fixed assets through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FixedAssetApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetApprovalApplication.class);

    private final AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking;

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync;

    private final RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final FixedAssetApprovalRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final AssetsRepository repository;

    public FixedAssetApprovalApplication(
            AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking,
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync,
            RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            FixedAssetApprovalRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            AssetsRepository repository) {
        this.assetsAssetTransferReportingServiceBlocking = assetsAssetTransferReportingServiceBlocking;
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insurancePremiumSyncServiceAsync = insurancePremiumSyncServiceAsync;
        this.lendingRepaymentLifecycleServiceBlocking = lendingRepaymentLifecycleServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Submits a fixed asset.
     */
    public SubmitFixedAssetResponse submitFixedAsset(SubmitFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getFixedAsset().getOwner().getEmail())
                        .build())
                .build();
        var stream0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound0);
        var reply0 = WatchDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SuggestUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getUpdatedAt())
                        .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                            case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestUsers(outbound1);

        var outbound2 = CheckNotificationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckNotificationEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            CheckNotificationEligibilityResponse fetch(CheckNotificationEligibilityRequest input) {
                return notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkNotificationEligibility(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input3 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamPremiumChangesRequest.newBuilder()
                .setChange(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFixedAsset().getAudit().getCreatedBy())
                                .setEtag(request.getFixedAsset().getAudit().getEtag())
                                .setUpdatedAt(request.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamPremiumChangesResponse.getDefaultInstance());
        var input4 = insurancePremiumSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitFixedAsset", request.getFixedAsset().getFixedAssetId());
        return SubmitFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getNotification().getAudit().getUpdatedBy())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(request.getFixedAsset().getOwner())
                        .build())
                .setOutcome(SubmitFixedAssetResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a fixed asset.
     */
    public ApproveFixedAssetResponse approveFixedAsset(ApproveFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = WatchAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAccessPolicies(outbound0);
        var reply0 = WatchAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SubmitClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SubmitClaimRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<SubmitClaimRequest, SubmitClaimResponse> port1 = RpcPort.adapt(insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitClaim);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveFixedAsset", request.getFixedAsset().getFixedAssetId());
        return ApproveFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getClaim().getRequester().getEmail())
                                .build())
                        .setStatus(request.getFixedAsset().getStatus())
                        .build())
                .setOutcome(ApproveFixedAssetResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a fixed asset.
     */
    public RejectFixedAssetResponse rejectFixedAsset(RejectFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getCreatedAt())
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var stream0 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound0);
        var reply0 = ExportUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = LintNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(request.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintNotifications(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectFixedAsset", request.getFixedAsset().getFixedAssetId());
        return RejectFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getNotification().getAudit().getEtag())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getNotification().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setOutcome(RejectFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Escalates a fixed asset.
     */
    public EscalateFixedAssetResponse escalateFixedAsset(EscalateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = CountAssetTransferTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAssetTransferTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getCreatedAt())
                        .setOwnerId(request.getFixedAsset().getOwner().getUserId())
                        .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                            case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAssetTransferTotals(outbound0);

        var outbound1 = ValidateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateUserRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getFixedAsset().getAudit().getCreatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getPostalCode();
                                    default -> "";
                                })
                                .setRecipient(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUser(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateFixedAsset", request.getFixedAsset().getFixedAssetId());
        return EscalateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getUser().getAudit().getEtag())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(EscalateFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public ListPendingFixedAssetsResponse listPendingFixedAssets(ListPendingFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "ListPendingFixedAssets");

        var outbound0 = SummarizeMetricSeriesesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SummarizeMetricSeriesesRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMetricSerieses(value)).orElseThrow();

        var outbound1 = ShareDisposalReportRequest.newBuilder()
                .putAllAnnotations(reply0.getResult().getDetailsMap())
                .setContext(request.getContext())
                .setDetails(ShareDisposalReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.assets.v1.DisposalReportingService/ShareDisposalReport", outbound1);
        var reply1 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDisposalReport(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingFixedAssets", request.getPage().getPageToken());
        return ListPendingFixedAssetsResponse.newBuilder()
                .addFixedAssets(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getDisposal().getRequester().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a fixed asset.
     */
    public DelegateFixedAssetApprovalResponse delegateFixedAssetApproval(DelegateFixedAssetApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateFixedAssetApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = EscalatePaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getFixedAsset().getAddress().getLinesList())
                                .setRegion(request.getFixedAsset().getAddress().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getFixedAsset().getOwner().getUserId())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalatePaymentPromise(outbound0);

        var outbound1 = StreamRepaymentMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRepaymentMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPaymentPromise().getAudit().getUpdatedAt())
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getPaymentPromise().getOrganizer().getEmail())
                        .build())
                .build();
        var stream1 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRepaymentMetrics(outbound1);
        var reply1 = StreamRepaymentMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateFixedAssetApproval", request.getFixedAsset().getFixedAssetId());
        return DelegateFixedAssetApprovalResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(request.getFixedAsset().getName())
                        .build())
                .setOutcome(DelegateFixedAssetApprovalResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a fixed asset.
     */
    public RecallFixedAssetResponse recallFixedAsset(RecallFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = mapper.recallFixedAssetStage0(request);
        var reply0 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDisposal(outbound0);

        var outbound1 = NormalizeUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDisposal().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeUser(outbound1);

        var outbound2 = ListVehiclesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVehiclesRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.fleet.v1.VehicleService");
        var serviceName2 = "acme.fleet.v1" + "." + "VehicleService";
        var methodName2 = "Lis" + "tVehicles";
        var descriptor2 = MethodDescriptor.<ListVehiclesRequest, ListVehiclesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ListVehiclesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListVehiclesResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ScheduleRepaymentExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleRepaymentExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getUser().getAudit().getCreatedBy())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getDisposal().getAssignee().getEmail())
                                        .setName(reply0.getDisposal().getRequester().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = lendingRepaymentLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRepaymentExpiry(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallFixedAsset", request.getFixedAsset().getFixedAssetId());
        return RecallFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(request.getFixedAsset().getName())
                        .build())
                .setOutcome(RecallFixedAssetResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public GetFixedAssetApprovalStatusResponse getFixedAssetApprovalStatus(GetFixedAssetApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFixedAssetApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "GetFixedAssetApprovalStatus");

        var outbound0 = ScheduleAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleAuditTrailExportRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        final class Stage0Adapter {
            ScheduleAuditTrailExportResponse fetch(ScheduleAuditTrailExportRequest input) {
                return auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditTrailExport(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetFixedAssetApprovalStatusResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Comments a fixed asset.
     */
    public CommentFixedAssetApprovalResponse commentFixedAssetApproval(CommentFixedAssetApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentFixedAssetApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var builder0 = NormalizeDisposalRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDisposal(Disposal.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setDisplayName(request.getFixedAsset().getOwner().getDisplayName())
                        .setUserId(request.getFixedAsset().getOwner().getUserId())
                        .build())
                .addComments(DisposalComment.newBuilder()
                        .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setEmail(request.getFixedAsset().getOwner().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDisposal(outbound0);

        var outbound1 = CheckUserEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> reply0.getDisposal().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkUserEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentFixedAssetApproval", request.getFixedAsset().getFixedAssetId());
        return CommentFixedAssetApprovalResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(request.getFixedAsset().getOwner())
                        .build())
                .setOutcome(CommentFixedAssetApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
