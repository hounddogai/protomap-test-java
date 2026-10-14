package com.acme.finance.domains.risk;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.RecalculateCorporateEventsRequest;
import com.acme.corporateevents.v1.RecalculateCorporateEventsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.CompareExposureOptionsRequest;
import com.acme.risk.v1.CompareExposureOptionsResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetExposureRatesRequest;
import com.acme.risk.v1.GetExposureRatesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.QuoteExposureRequest;
import com.acme.risk.v1.QuoteExposureResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.SimulateExposureRequest;
import com.acme.risk.v1.SimulateExposureResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an exposure, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExposureCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExposureCalculationApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final RiskRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    public ExposureCalculationApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            RiskRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
    }

    public record ExplainExposureCalculationStage0Requested(SimulateLoanRequest request, CompletableFuture<SimulateLoanResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an exposure.
     */
    public CalculateExposureResponse calculateExposure(CalculateExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getExposure().getContact().getName())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(switch (request.getExposure().getFundingCase()) {
                                    case BANK -> request.getExposure().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .setPreferredChannel(request.getExposure().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateExposure", request.getExposure().getExposureId());
        return CalculateExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getPolicy().getContact().getName())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(request.getExposure().getHolder())
                        .build())
                .build();
    }

    /**
     * Estimates an exposure.
     */
    public EstimateExposureResponse estimateExposure(EstimateExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RecalculateCorporateEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setCorporateEvent(CorporateEvent.newBuilder()
                        .addParticipants(CorporateEventParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getExposure().getContact().getEmail())
                                        .setName(request.getExposure().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .setDetails(RecalculateCorporateEventsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.corporateevents.v1.CorporateEventCalculationService");
        var serviceName0 = "acme.corporateevents.v1" + "." + "CorporateEventCalculationService";
        var methodName0 = "Rec" + "alculateCorporateEvents";
        var descriptor0 = MethodDescriptor.<RecalculateCorporateEventsRequest, RecalculateCorporateEventsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateCorporateEventsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateCorporateEventsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var results1 = new ArrayList<PreviewRiskRegisterResponse>();
        for (var element : request.getExposure().getLimitsList()) {
            PreviewRiskRegisterRequest itemRequest = PreviewRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .addChildren(RiskRegister.newBuilder()
                                                .setName(element.getName())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                                .setLocality(reply0.getCorporateEvent().getLocation().getLocality())
                                                .setRegion(reply0.getCorporateEvent().getLocation().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskRegister(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PreviewRiskRegisterResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateExposure", request.getExposure().getExposureId());
        return EstimateExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(reply0.getCorporateEvent().getOrganizer().getEmail())
                                .build())
                        .setStatus(request.getExposure().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes an exposure.
     */
    public QuoteExposureResponse quoteExposure(QuoteExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = ValidatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePremium(outbound0), rpcExecutor);

        var outbound1 = CheckAuditEventEligibilityRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckAuditEventEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .setName(request.getExposure().getContact().getName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAuditEventEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CompareCollateralOptionsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setLocality(reply0.getPremium().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCollateralOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPremium().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply2 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCollateralOptions(outbound2);

        var results3 = new ArrayList<NormalizeFeatureFlagResponse>();
        for (var element : request.getExposure().getLimitsList()) {
            NormalizeFeatureFlagRequest itemRequest = NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .build())
                        .setDiscount(FeatureFlagDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .setName(element.getName())
                        .build())
                .build();
            results3.add(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeFeatureFlag(itemRequest));
        }
        var reply3 = results3.isEmpty() ? NormalizeFeatureFlagResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteExposure", request.getExposure().getExposureId());
        return QuoteExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setBalance(request.getExposure().getBalance())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares an exposure.
     */
    public CompareExposureOptionsResponse compareExposureOptions(CompareExposureOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareExposureOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RecalculateMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateMandatesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setLast4(switch (request.getExposure().getFundingCase()) {
                                    case CARD -> request.getExposure().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound0);

        var outbound1 = ValidateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ValidateSearchIndexResponse fetch(ValidateSearchIndexRequest input) {
                return searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSearchIndex(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = JsonCodec.roundTrip(PreviewRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                                .setLocality(request.getDetails().getDestination().getLocality())
                                                .setPostalCode(reply0.getMandate().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), PreviewRiskRegisterRequest.class);
        var reply2 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskRegister(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareExposureOptions", request.getExposure().getExposureId());
        return CompareExposureOptionsResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSearchIndex().getAudit().getCreatedAt())
                                .build())
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates an exposure.
     */
    public RecalculateExposuresResponse recalculateExposures(RecalculateExposuresRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateExposures")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<LintRiskRegistersRequest, LintRiskRegistersResponse> operation0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintRiskRegisters;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PreviewRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getDetails().getDestination().getLocality())
                                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskRegister(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()), CreateUserRequest.class);
        var reply2 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createUser(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateExposures", request.getExposure().getExposureId());
        return RecalculateExposuresResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRiskRegister().getManager().getEmail())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setStatus(request.getExposure().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates an exposure.
     */
    public SimulateExposureResponse simulateExposure(SimulateExposureRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateExposure")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainRiskRegisterDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                                .setEtag(request.getExposure().getAudit().getEtag())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()), ExplainRiskRegisterDecisionRequest.class);
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskRegisterDecision(outbound0);

        var results1 = new ArrayList<ValidateRiskRegisterResponse>();
        for (var element : request.getExposure().getLimitsList()) {
            ValidateRiskRegisterRequest itemRequest = ValidateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setName(element.getName())
                                .build())
                        .build())
                .build();
            results1.add(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskRegister(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ValidateRiskRegisterResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = JsonCodec.roundTrip(PreviewRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(request.getExposure().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(reply1.getRiskRegister().getLocation().getPostalCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getExposure().getHolder().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), PreviewRiskRegisterRequest.class);
        var reply2 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskRegister(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateExposure", request.getExposure().getExposureId());
        return SimulateExposureResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(request.getExposure().getContact())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getRiskRegister().getManager().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains an exposure.
     */
    public ExplainExposureCalculationResponse explainExposureCalculation(ExplainExposureCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExposureCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .build())
                        .setCard(LoanCardFunding.newBuilder()
                                .setNetwork(request.getExposure().getCard().getNetwork())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SimulateLoanResponse>();
        events.publishEvent(new ExplainExposureCalculationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        List<AnnotateRetentionPolicyVersionResponse> results1 = request.getExposure().getLimitsList().stream()
                .map(element -> AnnotateRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getLoan().getAudit().getEtag())
                                .build())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .setName(element.getName())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateRetentionPolicyVersion(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? AnnotateRetentionPolicyVersionResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExposureCalculation", request.getExposure().getExposureId());
        return ExplainExposureCalculationResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getLoan().getContact().getName())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(request.getExposure().getHolder())
                        .build())
                .build();
    }

    /**
     * Returns an exposure.
     */
    public GetExposureRatesResponse getExposureRates(GetExposureRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExposureRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Exposure", request.getExposureId(), "GetExposureRates");
        return GetExposureRatesResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .build())
                .build();
    }
}
