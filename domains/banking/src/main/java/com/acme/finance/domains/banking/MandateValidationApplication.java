package com.acme.finance.domains.banking;

import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetMandateRatesRequest;
import com.acme.banking.v1.GetMandateRatesResponse;
import com.acme.banking.v1.GetMandateRulesRequest;
import com.acme.banking.v1.GetMandateRulesResponse;
import com.acme.banking.v1.LintMandatesRequest;
import com.acme.banking.v1.LintMandatesResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks mandates against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class MandateValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(MandateValidationApplication.class);

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final MandateValidationRequestMapper mapper;

    private final BankingRepository repository;

    private final Executor rpcExecutor;

    public MandateValidationApplication(
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ApplicationEventPublisher events,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            MandateValidationRequestMapper mapper,
            BankingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.events = events;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ExplainMandateDecisionStage0Requested(NormalizeClaimRequest request, CompletableFuture<NormalizeClaimResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a mandate.
     */
    public ValidateMandateResponse validateMandate(ValidateMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = LintStandingOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMandate().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintStandingOrders(outbound0), rpcExecutor);

        var outbound1 = CompareCoverageOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getMandate().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCoverageOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            PreviewPremiumResponse fetch(PreviewPremiumRequest input) {
                return insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPremium(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = BeanCopies.copy(ValidateClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateClaimRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build(), ValidateClaimRequest.class);
        var reply3 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateMandate", request.getMandate().getMandateId());
        return ValidateMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStandingOrder().getLocation().getCountryCode())
                                .setRegion(reply0.getStandingOrder().getLocation().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getCoverage().getOwner().getEmail())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setDescription(reply1.getCoverage().getDescription())
                                .build())
                        .setMandateId(request.getMandateId())
                        .setNotes(request.getMandate().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a mandate.
     */
    public PreviewMandateResponse previewMandate(PreviewMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ExplainStandingOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainStandingOrderDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainStandingOrderDecisionRequest, ExplainStandingOrderDecisionResponse> port0 = RpcPort.adapt(bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainStandingOrderDecision);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewMandate", request.getMandate().getMandateId());
        return PreviewMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .setNotes(request.getMandate().getNotes())
                        .build())
                .setOutcome(PreviewMandateResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes a mandate.
     */
    public NormalizeMandateResponse normalizeMandate(NormalizeMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var builder0 = CompareMandateOptionsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CompareMandateOptionsRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .addAllLines(request.getMandate().getBillingAddress().getLinesList())
                        .build())
                .build());
        builder0.setMandate(Mandate.newBuilder()
                .setCard(MandateCardSettlement.newBuilder()
                        .setToken(request.getMandate().getCard().getToken())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareMandateOptions(outbound0);

        var outbound1 = mapper.normalizeMandateStage1(request);
        var reply1 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeMandate", request.getMandate().getMandateId());
        return NormalizeMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getClaim().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setMandateId(request.getMandateId())
                        .setTotal(request.getMandate().getTotal())
                        .build())
                .build();
    }

    /**
     * Explains a mandate.
     */
    public ExplainMandateDecisionResponse explainMandateDecision(ExplainMandateDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainMandateDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = NormalizeClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setCustomerId(request.getMandate().getCounterparty().getCustomerId())
                                .setDisplayName(request.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeClaimRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getMandate().getCounterparty().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<NormalizeClaimResponse>();
        events.publishEvent(new ExplainMandateDecisionStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainMandateDecision", request.getMandate().getMandateId());
        return ExplainMandateDecisionResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .setTotal(request.getMandate().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Scores a mandate.
     */
    public ScoreMandateResponse scoreMandate(ScoreMandateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreMandate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = (GetMandateRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setMandateId(request.getMandate().getMandateId())
                .build()).toBuilder().build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getMandateRates(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreMandate", request.getMandate().getNumber());
        return ScoreMandateResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .build())
                        .setDueAt(request.getMandate().getDueAt())
                        .setMandateId(request.getMandateId())
                        .build())
                .build();
    }

    /**
     * Lints a mandate.
     */
    public LintMandatesResponse lintMandates(LintMandatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintMandates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMandate(request.getContext().getTenantId(), request.getMandate());

        var outbound0 = ValidateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ValidateStandingOrderResponse fetch(ValidateStandingOrderRequest input) {
                return bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMandate().getMandateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintMandates", request.getMandate().getMandateId());
        return LintMandatesResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getStandingOrder().getAudit().getCreatedAt())
                                .build())
                        .setDueAt(request.getMandate().getDueAt())
                        .setMandateId(request.getMandateId())
                        .build())
                .setOutcome(LintMandatesResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a mandate.
     */
    public GetMandateRulesResponse getMandateRules(GetMandateRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMandateRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Mandate", request.getMandateId(), "GetMandateRules");
        return GetMandateRulesResponse.newBuilder()
                .setMandate(Mandate.newBuilder()
                        .setMandateId(request.getMandateId())
                        .build())
                .build();
    }
}
