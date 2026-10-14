package com.acme.finance.domains.banking;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.GetMandateRatesRequest;
import com.acme.banking.v1.GetMandateRatesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.BatchGetArticlesRequest;
import com.acme.content.v1.BatchGetArticlesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.experiments.v1.Experiment;
import com.acme.experiments.v1.ExperimentCalculationServiceGrpc;
import com.acme.experiments.v1.ExperimentParticipant;
import com.acme.experiments.v1.ExplainExperimentCalculationRequest;
import com.acme.experiments.v1.ExplainExperimentCalculationResponse;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.fuel.v1.FuelCardServiceGrpc;
import com.acme.fuel.v1.ListFuelCardsRequest;
import com.acme.fuel.v1.ListFuelCardsResponse;
import com.acme.fuel.v1.WatchFuelCardsRequest;
import com.acme.fuel.v1.WatchFuelCardsResponse;
import com.acme.helpdesk.v1.ResolveTicketsRequest;
import com.acme.helpdesk.v1.ResolveTicketsResponse;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScorePremiumRequest;
import com.acme.insurance.v1.ScorePremiumResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.media.v1.MediaAssetQueryServiceGrpc;
import com.acme.media.v1.SummarizeMediaAssetsRequest;
import com.acme.media.v1.SummarizeMediaAssetsResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a mandate, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class MandateCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(MandateCalculationApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final ExperimentCalculationServiceGrpc.ExperimentCalculationServiceBlockingStub experimentsExperimentCalculationServiceBlocking;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final FuelCardServiceGrpc.FuelCardServiceBlockingStub fuelFuelCardServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final MediaAssetQueryServiceGrpc.MediaAssetQueryServiceBlockingStub mediaMediaAssetQueryServiceBlocking;

    private final BankingRepository repository;

    private final Executor rpcExecutor;

    public MandateCalculationApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            ExperimentCalculationServiceGrpc.ExperimentCalculationServiceBlockingStub experimentsExperimentCalculationServiceBlocking,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            FuelCardServiceGrpc.FuelCardServiceBlockingStub fuelFuelCardServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            MediaAssetQueryServiceGrpc.MediaAssetQueryServiceBlockingStub mediaMediaAssetQueryServiceBlocking,
            BankingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.experimentsExperimentCalculationServiceBlocking = experimentsExperimentCalculationServiceBlocking;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.fuelFuelCardServiceBlocking = fuelFuelCardServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.mediaMediaAssetQueryServiceBlocking = mediaMediaAssetQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a mandate.
     */
    public CalculateMandateResponse calculateMandate(CalculateMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ListFuelCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFuelCardsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMandate().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> fuelFuelCardServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFuelCards(outbound0), rpcExecutor);

        var outbound1 = ExplainCoverageDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(ExplainCoverageDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageDecision(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BeanCopies.copy(CheckPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build(), CheckPolicyEligibilityRequest.class);
        var reply2 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPolicyEligibility(outbound2);

        var outbound3 = CountPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getPolicy().getAudit().getCreatedAt())
                        .putAllLabels(reply2.getPolicy().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPolicies(value));
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateMandate", request.getMandate().getMandateId());
        return CalculateMandateResponse.newBuilder()
                .setBreakdown(CalculateMandateResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getCoverage().getOwner().getEmail())
                                .build())
                        .setMandateId(request.getMandateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates a mandate.
     */
    public EstimateMandateResponse estimateMandate(EstimateMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = JsonCodec.roundTrip(SummarizeMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build(), SummarizeMediaAssetsRequest.class);
        var reply0 = mediaMediaAssetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMediaAssets(outbound0);

        var outbound1 = CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMandate().getAudit().getCreatedAt())
                                .setEtag(request.getMandate().getAudit().getEtag())
                                .setUpdatedAt(request.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<CloneAccessPolicyRequest, CloneAccessPolicyResponse> operation1 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneAccessPolicy;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateMandate", request.getMandate().getMandateId());
        return EstimateMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getAccessPolicy().getAudit().getUpdatedBy())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(reply1.getAccessPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setMandateId(request.getMandateId())
                        .putAllMetadata(request.getMandate().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Quotes a mandate.
     */
    public QuoteMandateResponse quoteMandate(QuoteMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ResolveTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.helpdesk.v1.TicketQueryService");
        var selected0 = TicketQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.resolveTickets(outbound0);

        var outbound1 = (ExplainExperimentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setExperiment(Experiment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMandate().getAudit().getEtag())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getMandate().getBillingAddress().getLocality())
                                .build())
                        .addParticipants(ExperimentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = experimentsExperimentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExperimentCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteMandate", request.getMandate().getMandateId());
        return QuoteMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .putAllMetadata(reply1.getExperiment().getMetadataMap())
                        .setNotes(request.getMandate().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Compares a mandate.
     */
    public CompareMandateOptionsResponse compareMandateOptions(CompareMandateOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareMandateOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getMandate().getBillingAddress().getLinesList())
                                .setRecipient(request.getMandate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<SimulateCoverageRequest, SimulateCoverageResponse> port0 = RpcPort.adapt(insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateCoverage);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<CheckCoverageEligibilityResponse>();
        for (var element : request.getMandate().getLinesList()) {
            CheckCoverageEligibilityRequest itemRequest = CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build();
            results1.add(insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCoverageEligibility(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CheckCoverageEligibilityResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ScorePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.PremiumValidationService");
        var serviceName2 = "acme.insurance.v1" + "." + "PremiumValidationService";
        var methodName2 = "Sco" + "rePremium";
        var descriptor2 = MethodDescriptor.<ScorePremiumRequest, ScorePremiumResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ScorePremiumRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScorePremiumResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ExplainUnderwritingCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.UnderwritingCalculationService");
        var serviceName3 = "acme.insurance.v1" + "." + "UnderwritingCalculationService";
        var methodName3 = "Exp" + "lainUnderwritingCalculation";
        var descriptor3 = MethodDescriptor.<ExplainUnderwritingCalculationRequest, ExplainUnderwritingCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainUnderwritingCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainUnderwritingCalculationResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareMandateOptions", request.getMandate().getMandateId());
        return CompareMandateOptionsResponse.newBuilder()
                .setBreakdown(CompareMandateOptionsResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getPremium().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply3.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setIssuedAt(reply2.getPremium().getIssuedAt())
                        .setMandateId(request.getMandateId())
                        .setTotal(request.getMandate().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalculates a mandate.
     */
    public RecalculateMandatesResponse recalculateMandates(RecalculateMandatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateMandates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var builder0 = ResolvePoliciesRequest.newBuilder();
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePolicies(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateMandates", request.getMandate().getMandateId());
        return RecalculateMandatesResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(request.getMandate().getCounterparty())
                        .setMandateId(request.getMandateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains a mandate.
     */
    public ExplainMandateCalculationResponse explainMandateCalculation(ExplainMandateCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainMandateCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = BatchGetArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.content.v1.ArticleService");
        var selected0 = ArticleServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.batchGetArticles(outbound0);

        var outbound1 = ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFeatureFlag(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainMandateCalculation", request.getMandate().getMandateId());
        return ExplainMandateCalculationResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setIssuedAt(request.getMandate().getIssuedAt())
                        .setMandateId(request.getMandateId())
                        .build())
                .setOutcome(ExplainMandateCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a mandate.
     */
    public GetMandateRatesResponse getMandateRates(GetMandateRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMandateRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Mandate", request.getMandateId(), "GetMandateRates");

        var outbound0 = WatchFuelCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setSince(request.getContext().getDeadline())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = fuelFuelCardServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFuelCards(outbound0);
        var reply0 = WatchFuelCardsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        return GetMandateRatesResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .build())
                .build();
    }
}
