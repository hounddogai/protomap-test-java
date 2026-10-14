package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowseClaimsRequest;
import com.acme.insurance.v1.BrowseClaimsResponse;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExportClaimsRequest;
import com.acme.insurance.v1.ExportClaimsResponse;
import com.acme.insurance.v1.LintPremiumsRequest;
import com.acme.insurance.v1.LintPremiumsResponse;
import com.acme.insurance.v1.LookupClaimRequest;
import com.acme.insurance.v1.LookupClaimResponse;
import com.acme.insurance.v1.PeekClaimRequest;
import com.acme.insurance.v1.PeekClaimResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SuggestClaimsRequest;
import com.acme.insurance.v1.SuggestClaimsResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.learning.v1.Course;
import com.acme.learning.v1.CourseComponent;
import com.acme.learning.v1.CourseServiceGrpc;
import com.acme.learning.v1.UpdateCourseRequest;
import com.acme.learning.v1.UpdateCourseResponse;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.NormalizeMetricSeriesRequest;
import com.acme.metrics.v1.NormalizeMetricSeriesResponse;
import com.acme.recommendations.v1.CreateRecommendationSetRequest;
import com.acme.recommendations.v1.CreateRecommendationSetResponse;
import com.acme.recommendations.v1.RecommendationSet;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about claims, for screens and reports. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ClaimQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimQueryApplication.class);

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final CourseServiceGrpc.CourseServiceBlockingStub learningCourseServiceBlocking;

    private final ClaimQueryRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public ClaimQueryApplication(
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            CourseServiceGrpc.CourseServiceBlockingStub learningCourseServiceBlocking,
            ClaimQueryRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.learningCourseServiceBlocking = learningCourseServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.recommendationsRecommendationSetServiceBlocking = recommendationsRecommendationSetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record LookupClaimStage0Requested(PreviewPremiumRequest request, CompletableFuture<PreviewPremiumResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a claim.
     */
    public LookupClaimResponse lookupClaim(LookupClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "LookupClaim");

        var outbound0 = PreviewPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPremiumRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(PremiumLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<PreviewPremiumResponse>();
        events.publishEvent(new LookupClaimStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return LookupClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .build();
    }

    /**
     * Lists claims a page at a time.
     */
    public BrowseClaimsResponse browseClaims(BrowseClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "BrowseClaims");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseClaims", request.getPage().getPageToken());
        return BrowseClaimsResponse.getDefaultInstance();
    }

    /**
     * Counts claims by status.
     */
    public CountClaimsResponse countClaims(CountClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "CountClaims");

        var outbound0 = NormalizeMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.metrics.v1.MetricSeriesValidationService");
        var selected0 = MetricSeriesValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.normalizeMetricSeries(outbound0);

        var outbound1 = SimulateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getMetricSeries().getOwner().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(switch (reply0.getMetricSeries().getLocatorCase()) {
                                    case ADDRESS -> reply0.getMetricSeries().getAddress().getPostalCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountClaims", request.getPage().getPageToken());
        return CountClaimsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several claims at once.
     */
    public ResolveClaimsResponse resolveClaims(ResolveClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "ResolveClaims");

        var outbound0 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(outbound0);
        return ResolveClaimsResponse.newBuilder()
                .addClaims(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Streams every matching claim.
     */
    public ExportClaimsResponse exportClaims(ExportClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "ExportClaims");

        var outbound0 = JsonCodec.roundTrip(LintPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), LintPremiumsRequest.class);
        var reply0 = insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPremiums(outbound0);

        var outbound1 = ScoreUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreUnderwritingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply0.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPremium().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ScoreUnderwritingRequest, ScoreUnderwritingResponse>() {
            @Override
            public ScoreUnderwritingResponse apply(ScoreUnderwritingRequest input) {
                return insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreUnderwriting(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportClaims", request.getFilter().getQuery());
        return ExportClaimsResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(reply1.getUnderwriting().getCustomer().getPriority())
                        .build())
                .build();
    }

    /**
     * Summarizes a claim.
     */
    public SummarizeClaimsResponse summarizeClaims(SummarizeClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "SummarizeClaims");

        var outbound0 = FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .putAllLabels(request.getAnnotationsMap())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(outbound0), rpcExecutor);

        var outbound1 = CountMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMetricSeriesesRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countMetricSerieses(outbound1), rpcExecutor);

        var outbound2 = UpdateCourseRequest.newBuilder()
                .setContext(request.getContext())
                .setCourse(Course.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .putComponents("primary", CourseComponent.newBuilder()
                                .setComponentId(request.getClaimId())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> learningCourseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCourse(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        var outbound3 = PreviewClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply2.getCourse().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getCourse().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeClaims", request.getDetails().getFormat());
        return SummarizeClaimsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists claims a page at a time.
     */
    public SuggestClaimsResponse suggestClaims(SuggestClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "SuggestClaims");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestClaims", request.getPage().getPageToken());
        return SuggestClaimsResponse.getDefaultInstance();
    }

    /**
     * Returns a claim.
     */
    public PeekClaimResponse peekClaim(PeekClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "PeekClaim");

        var outbound0 = mapper.peekClaimStage0(request);
        var reply0 = recommendationsRecommendationSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createRecommendationSet(outbound0);

        var outbound1 = ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRecommendationSet().getAudit().getEtag())
                                .setUpdatedAt(reply0.getRecommendationSet().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getRecommendationSet().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.CoverageCalculationService");
        var serviceName1 = "acme.insurance.v1" + "." + "CoverageCalculationService";
        var methodName1 = "Exp" + "lainCoverageCalculation";
        var descriptor1 = MethodDescriptor.<ExplainCoverageCalculationRequest, ExplainCoverageCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainCoverageCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainCoverageCalculationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        return PeekClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getRecommendationSet().getCounterparty().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
