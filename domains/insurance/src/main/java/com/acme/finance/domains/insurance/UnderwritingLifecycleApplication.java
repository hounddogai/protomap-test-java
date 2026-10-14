package com.acme.finance.domains.insurance;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.ResolveAnalyticsEventsRequest;
import com.acme.analytics.v1.ResolveAnalyticsEventsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateUnderwritingRequest;
import com.acme.insurance.v1.ActivateUnderwritingResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.CountUnderwritingTotalsRequest;
import com.acme.insurance.v1.CountUnderwritingTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CreatePolicyRequest;
import com.acme.insurance.v1.CreatePolicyResponse;
import com.acme.insurance.v1.ExpireUnderwritingRequest;
import com.acme.insurance.v1.ExpireUnderwritingResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.GetUnderwritingLifecycleRequest;
import com.acme.insurance.v1.GetUnderwritingLifecycleResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.ReinstateUnderwritingRequest;
import com.acme.insurance.v1.ReinstateUnderwritingResponse;
import com.acme.insurance.v1.RenewUnderwritingRequest;
import com.acme.insurance.v1.RenewUnderwritingResponse;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryRequest;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.SuspendUnderwritingRequest;
import com.acme.insurance.v1.SuspendUnderwritingResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.TerminateUnderwritingRequest;
import com.acme.insurance.v1.TerminateUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves an underwriting between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class UnderwritingLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(UnderwritingLifecycleApplication.class);

    private final AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceFutureStub insurancePremiumReportingServiceFuture;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public UnderwritingLifecycleApplication(
            AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceFutureStub insurancePremiumReportingServiceFuture,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.analyticsAnalyticsEventQueryServiceBlocking = analyticsAnalyticsEventQueryServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insurancePremiumReportingServiceFuture = insurancePremiumReportingServiceFuture;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    /**
     * Activates an underwriting.
     */
    public ActivateUnderwritingResponse activateUnderwriting(ActivateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "ActivateUnderwriting");

        var outbound0 = RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.CustomerCalculationService");
        var serviceName0 = "acme.customers.v1" + "." + "CustomerCalculationService";
        var methodName0 = "Rec" + "alculateCustomers";
        var descriptor0 = MethodDescriptor.<RecalculateCustomersRequest, RecalculateCustomersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateCustomersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateCustomersResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCustomer().getPrimaryAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            UpdateMetricSeriesResponse fetch(UpdateMetricSeriesRequest input) {
                return metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateMetricSeries(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateUnderwriting", request.getDetails().getReasonCode());
        return ActivateUnderwritingResponse.newBuilder()
                .setOutcome(ActivateUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .addAllTags(reply1.getMetricSeries().getTagsList())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Suspends an underwriting.
     */
    public SuspendUnderwritingResponse suspendUnderwriting(SuspendUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "SuspendUnderwriting");

        var outbound0 = SuggestSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSearchIndexes(outbound0), rpcExecutor);

        var outbound1 = QuoteMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteMandate(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendUnderwriting", request.getDetails().getReasonCode());
        return SuspendUnderwritingResponse.newBuilder()
                .setOutcome(SuspendUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Reinstates an underwriting.
     */
    public ReinstateUnderwritingResponse reinstateUnderwriting(ReinstateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "ReinstateUnderwriting");

        var outbound0 = BeanCopies.copy(ListUnderwritingConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getUnderwritingId())
                        .build())
                .build(), ListUnderwritingConflictsRequest.class);
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingConflicts(outbound0);

        var outbound1 = GeneratePremiumReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GeneratePremiumReportRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = insurancePremiumReportingServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).generatePremiumReport(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = CountUnderwritingTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountUnderwritingTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.UnderwritingReportingService");
        var serviceName2 = "acme.insurance.v1" + "." + "UnderwritingReportingService";
        var methodName2 = "Cou" + "ntUnderwritingTotals";
        var descriptor2 = MethodDescriptor.<CountUnderwritingTotalsRequest, CountUnderwritingTotalsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CountUnderwritingTotalsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountUnderwritingTotalsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateUnderwriting", request.getDetails().getReasonCode());
        return ReinstateUnderwritingResponse.newBuilder()
                .setOutcome(ReinstateUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Terminates an underwriting.
     */
    public TerminateUnderwritingResponse terminateUnderwriting(TerminateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "TerminateUnderwriting");

        var outbound0 = JsonCodec.roundTrip(ScoreRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNewOwner().getEmail())
                                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build(), ScoreRenewalRequest.class);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRenewal(outbound0);

        var outbound1 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRenewal().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateUnderwriting", request.getDetails().getReasonCode());
        return TerminateUnderwritingResponse.newBuilder()
                .setOutcome(TerminateUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .setSla(UnderwritingSla.newBuilder()
                                .setDueAt(reply1.getPremium().getDueAt())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Renews an underwriting.
     */
    public RenewUnderwritingResponse renewUnderwriting(RenewUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "RenewUnderwriting");

        var outbound0 = JsonCodec.roundTrip(SharePremiumReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SharePremiumReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), SharePremiumReportRequest.class);
        var reply0 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sharePremiumReport(outbound0);

        var outbound1 = QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getPremium().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setDetails(QuoteCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getPremium().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            QuoteCoverageResponse fetch(QuoteCoverageRequest input) {
                return insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = CreatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setNetwork(switch (reply0.getPremium().getSettlementCase()) {
                                    case CARD -> reply0.getPremium().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createPolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewUnderwriting", request.getDetails().getReasonCode());
        return RenewUnderwritingResponse.newBuilder()
                .setOutcome(RenewUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getCoverage().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply2.getPolicy().getContact().getPhone())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Expires an underwriting.
     */
    public ExpireUnderwritingResponse expireUnderwriting(ExpireUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "ExpireUnderwriting");

        var outbound0 = SharePremiumReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SharePremiumReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sharePremiumReport(outbound0), rpcExecutor);

        var outbound1 = CommentPolicyApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPolicyApproval(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireUnderwriting", request.getDetails().getReasonCode());
        return ExpireUnderwritingResponse.newBuilder()
                .setOutcome(ExpireUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Returns an underwriting.
     */
    public GetUnderwritingLifecycleResponse getUnderwritingLifecycle(GetUnderwritingLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetUnderwritingLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "GetUnderwritingLifecycle");
        return GetUnderwritingLifecycleResponse.newBuilder()
                .setUnderwriting(Underwriting.newBuilder()
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Schedules an underwriting.
     */
    public ScheduleUnderwritingExpiryResponse scheduleUnderwritingExpiry(ScheduleUnderwritingExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleUnderwritingExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ApprovePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getUnderwriting().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePolicy(outbound0), rpcExecutor);

        var outbound1 = ResolveAnalyticsEventsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> analyticsAnalyticsEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAnalyticsEvents(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrailConflict(value));

        var builder3 = SyncPremiumsRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setDetails(SyncPremiumsRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder3.setPremium(Premium.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                        .build())
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(reply2.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .setCounterparty(SupplierRef.newBuilder()
                        .setEmail(request.getUnderwriting().getRequester().getEmail())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncPremiums(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleUnderwritingExpiry", request.getUnderwriting().getUnderwritingId());
        return ScheduleUnderwritingExpiryResponse.newBuilder()
                .setOutcome(ScheduleUnderwritingExpiryResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(request.getUnderwriting().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getPremium().getAudit().getEtag())
                                .setUpdatedBy(reply2.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }
}
