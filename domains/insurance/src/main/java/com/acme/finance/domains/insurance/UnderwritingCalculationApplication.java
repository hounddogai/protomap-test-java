package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.boxoffice.v1.BoxOfficeSale;
import com.acme.boxoffice.v1.BoxOfficeSaleServiceGrpc;
import com.acme.boxoffice.v1.CloneBoxOfficeSaleRequest;
import com.acme.boxoffice.v1.CloneBoxOfficeSaleResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.helpdesk.v1.SuggestTicketsRequest;
import com.acme.helpdesk.v1.SuggestTicketsResponse;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.CalculateUnderwritingRequest;
import com.acme.insurance.v1.CalculateUnderwritingResponse;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.GetUnderwritingRatesRequest;
import com.acme.insurance.v1.GetUnderwritingRatesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.QuoteUnderwritingRequest;
import com.acme.insurance.v1.QuoteUnderwritingResponse;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.RecalculateUnderwritingsRequest;
import com.acme.insurance.v1.RecalculateUnderwritingsResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.insurance.v1.SimulatePremiumRequest;
import com.acme.insurance.v1.SimulatePremiumResponse;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationComment;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ScoreNotificationRequest;
import com.acme.notifications.v1.ScoreNotificationResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an underwriting, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class UnderwritingCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(UnderwritingCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BoxOfficeSaleServiceGrpc.BoxOfficeSaleServiceBlockingStub boxofficeBoxOfficeSaleServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final TicketQueryServiceGrpc.TicketQueryServiceBlockingStub helpdeskTicketQueryServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final UnderwritingCalculationRequestMapper mapper;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public UnderwritingCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BoxOfficeSaleServiceGrpc.BoxOfficeSaleServiceBlockingStub boxofficeBoxOfficeSaleServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            TicketQueryServiceGrpc.TicketQueryServiceBlockingStub helpdeskTicketQueryServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            UnderwritingCalculationRequestMapper mapper,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.boxofficeBoxOfficeSaleServiceBlocking = boxofficeBoxOfficeSaleServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.helpdeskTicketQueryServiceBlocking = helpdeskTicketQueryServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record SimulateUnderwritingStage3Requested(BrowseMetricSeriesesRequest request, CompletableFuture<BrowseMetricSeriesesResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an underwriting.
     */
    public CalculateUnderwritingResponse calculateUnderwriting(CalculateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (request.getUnderwriting().getOriginCase()) {
                                    case CUSTOMER -> request.getUnderwriting().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(CheckCoverageEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCoverageEligibility(outbound0);

        var futures1 = request.getUnderwriting().getCommentsList().stream()
                .map(element -> EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? EstimatePolicyResponse.getDefaultInstance()
                : results1.getLast();

        List<ExplainPolicyCalculationResponse> results2 = request.getUnderwriting().getCommentsList().stream()
                .map(element -> ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? ExplainPolicyCalculationResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = JsonCodec.roundTrip(ExplainCoverageDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainCoverageDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build(), ExplainCoverageDecisionRequest.class);
        var reply3 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageDecision(outbound3);

        var outbound4 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getUnderwriting().getRequester())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input4 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .setCreatedBy(request.getUnderwriting().getAudit().getCreatedBy())
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateUnderwriting", request.getUnderwriting().getUnderwritingId());
        return CalculateUnderwritingResponse.newBuilder()
                .setBreakdown(CalculateUnderwritingResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .addLines(CalculateUnderwritingResponse.Breakdown.Line.newBuilder()
                                .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply3.getCoverage().getOwner().getEmail())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Estimates an underwriting.
     */
    public EstimateUnderwritingResponse estimateUnderwriting(EstimateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = RpcSupport.unpack(Any.pack(LintPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .build())
                        .build())
                .build()), LintPoliciesRequest.class);
        var reply0 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPolicies(outbound0);

        var outbound1 = QuoteClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setDueAt(request.getUnderwriting().getSla().getDueAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteClaim(outbound1);

        var outbound2 = RecalculateCoveragesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateCoveragesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.CoverageCalculationService");
        var serviceName2 = "acme.insurance.v1" + "." + "CoverageCalculationService";
        var methodName2 = "Rec" + "alculateCoverages";
        var descriptor2 = MethodDescriptor.<RecalculateCoveragesRequest, RecalculateCoveragesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateCoveragesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateCoveragesResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateUnderwriting", request.getUnderwriting().getUnderwritingId());
        return EstimateUnderwritingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(reply1.getClaim().getCustomFieldsMap())
                        .setRequester(request.getUnderwriting().getRequester())
                        .setSubject(reply1.getClaim().getSubject())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Quotes an underwriting.
     */
    public QuoteUnderwritingResponse quoteUnderwriting(QuoteUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .build();
        RpcPort<QuotePolicyRequest, QuotePolicyResponse> port0 = RpcPort.adapt(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quotePolicy);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        Function<ExplainPolicyCalculationRequest, ExplainPolicyCalculationResponse> operation1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainPolicyCalculation;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = JsonCodec.roundTrip(CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), CalculatePolicyRequest.class);
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteUnderwriting", request.getUnderwriting().getUnderwritingId());
        return QuoteUnderwritingResponse.newBuilder()
                .setBreakdown(QuoteUnderwritingResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getPolicy().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .putAllCustomFields(request.getUnderwriting().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getPolicy().getContact().getEmail())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Compares an underwriting.
     */
    public CompareUnderwritingOptionsResponse compareUnderwritingOptions(CompareUnderwritingOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareUnderwritingOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyCalculationService");
        var serviceName0 = "acme.insurance.v1" + "." + "PolicyCalculationService";
        var methodName0 = "Rec" + "alculatePolicies";
        var descriptor0 = MethodDescriptor.<RecalculatePoliciesRequest, RecalculatePoliciesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculatePoliciesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculatePoliciesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getUnderwriting().getCommentsList().stream()
                .map(element -> SimulatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? SimulatePolicyResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareUnderwritingOptions", request.getUnderwriting().getUnderwritingId());
        return CompareUnderwritingOptionsResponse.newBuilder()
                .setBreakdown(CompareUnderwritingOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Recalculates an underwriting.
     */
    public RecalculateUnderwritingsResponse recalculateUnderwritings(RecalculateUnderwritingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateUnderwritings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = SimulatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePremiumRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(switch (request.getUnderwriting().getOriginCase()) {
                                    case CUSTOMER -> request.getUnderwriting().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePremium(outbound0);

        var outbound1 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (reply0.getPremium().getSettlementCase()) {
                                    case CARD -> reply0.getPremium().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(outbound1);

        var outbound2 = CalculateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply1.getPolicy().getContact().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CalculateClaimRequest, CalculateClaimResponse> port2 = RpcPort.adapt(insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateClaim);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateUnderwritings", request.getUnderwriting().getUnderwritingId());
        return RecalculateUnderwritingsResponse.newBuilder()
                .setBreakdown(RecalculateUnderwritingsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getPolicy().getHolder().getDisplayName())
                                .build())
                        .putAllCustomFields(request.getUnderwriting().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getClaim().getCustomer().getEmail())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Simulates an underwriting.
     */
    public SimulateUnderwritingResponse simulateUnderwriting(SimulateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = NotifyAuditTrailWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditTrailWatchersRequest.Details.newBuilder()
                        .setChannel(switch (request.getUnderwriting().getOriginCase()) {
                            case CHANNEL -> request.getUnderwriting().getChannel().getChannel();
                            default -> "";
                        })
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .setName(request.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply0 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditTrailWatchers(outbound0);

        var outbound1 = mapper.simulateUnderwritingStage1(request, reply0);
        var reply1 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteClaim(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAuditTrail().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getClaim().getCustomer().getDisplayName())
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .setDetails(CheckCoverageEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .build()), CheckCoverageEligibilityRequest.class);
        var reply2 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCoverageEligibility(outbound2);

        var outbound3 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getClaim().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getClaim().getAssignee().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion3 = new CompletableFuture<BrowseMetricSeriesesResponse>();
        events.publishEvent(new SimulateUnderwritingStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateUnderwriting", request.getUnderwriting().getUnderwritingId());
        return SimulateUnderwritingResponse.newBuilder()
                .setBreakdown(SimulateUnderwritingResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getCoverage().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Explains an underwriting.
     */
    public ExplainUnderwritingCalculationResponse explainUnderwritingCalculation(ExplainUnderwritingCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainUnderwritingCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var results0 = new ArrayList<CloneBoxOfficeSaleResponse>();
        for (var element : request.getUnderwriting().getCommentsList()) {
            CloneBoxOfficeSaleRequest itemRequest = CloneBoxOfficeSaleRequest.newBuilder()
                .setBoxOfficeSale(BoxOfficeSale.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setCreatedBy(request.getUnderwriting().getAudit().getCreatedBy())
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(boxofficeBoxOfficeSaleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneBoxOfficeSale(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CloneBoxOfficeSaleResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SimulatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePremiumRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getBoxOfficeSale().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBank(PremiumBankSettlement.newBuilder()
                                .setBic(reply0.getBoxOfficeSale().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getBoxOfficeSale().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePremium(value)).orElseThrow();

        var outbound2 = ScoreNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreNotificationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply1.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .addComments(NotificationComment.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.notifications.v1.NotificationValidationService/ScoreNotification", outbound2);
        var reply2 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreNotification(command2.payload());

        var outbound3 = mapper.explainUnderwritingCalculationStage3(request, reply1);
        var reply3 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainUnderwritingCalculation", request.getUnderwriting().getUnderwritingId());
        return ExplainUnderwritingCalculationResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply3.getClaim().getAssignee().getEmail())
                                .build())
                        .putAllCustomFields(reply2.getNotification().getCustomFieldsMap())
                        .setPriority(request.getUnderwriting().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getPremium().getCounterparty().getEmail())
                                .setName(reply2.getNotification().getRequester().getName())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Returns an underwriting.
     */
    public GetUnderwritingRatesResponse getUnderwritingRates(GetUnderwritingRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetUnderwritingRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "GetUnderwritingRates");

        var outbound0 = SuggestTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestTicketsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getUnderwritingId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> helpdeskTicketQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestTickets(outbound0), rpcExecutor);

        var outbound1 = ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(ExplainCoverageCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        return GetUnderwritingRatesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }
}
