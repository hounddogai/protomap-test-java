package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.CalculatePremiumRequest;
import com.acme.insurance.v1.CalculatePremiumResponse;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculateUnderwritingsRequest;
import com.acme.insurance.v1.RecalculateUnderwritingsResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExplainCollateralDecisionRequest;
import com.acme.lending.v1.ExplainCollateralDecisionResponse;
import com.acme.lending.v1.GetCollateralRulesRequest;
import com.acme.lending.v1.GetCollateralRulesResponse;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.ValidateCollateralRequest;
import com.acme.lending.v1.ValidateCollateralResponse;
import com.acme.maintenance.v1.BrowseWorkOrdersRequest;
import com.acme.maintenance.v1.BrowseWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrderQueryServiceGrpc;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ScoreNotificationRequest;
import com.acme.notifications.v1.ScoreNotificationResponse;
import com.acme.sponsorships.v1.EstimateSponsorRequest;
import com.acme.sponsorships.v1.EstimateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.acme.sponsorships.v1.SponsorNationalId;
import com.acme.venues.v1.NormalizeVenueRequest;
import com.acme.venues.v1.NormalizeVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks collaterals against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class CollateralValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(CollateralValidationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final LendingRepository repository;

    private final SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public CollateralValidationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            LendingRepository repository,
            SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.sponsorshipsSponsorCalculationServiceBlocking = sponsorshipsSponsorCalculationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Validates a collateral.
     */
    public ValidateCollateralResponse validateCollateral(ValidateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditEventNotificationService");
        var selected0 = AuditEventNotificationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.subscribeAuditEventUpdates(outbound0);

        var outbound1 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCollateral().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getCountryCode();
                                    default -> "";
                                })
                                .setLocality(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(outbound1);

        var outbound2 = QuoteRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteRepaymentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRegion(reply1.getMandate().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteRepayment(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateCollateral", request.getCollateral().getCollateralId());
        return ValidateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setCapacity(request.getCollateral().getCapacity())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply2.getRepayment().getOrganizer().getEmail())
                                .build())
                        .build())
                .setOutcome(ValidateCollateralResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a collateral.
     */
    public PreviewCollateralResponse previewCollateral(PreviewCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = (CalculatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePremium(outbound0);

        var outbound1 = ScoreNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getPremium().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreNotification(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewCollateral", request.getCollateral().getCollateralId());
        return PreviewCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .setName(request.getCollateral().getName())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getNotification().getAssignee().getDisplayName())
                                .setEmail(reply1.getNotification().getAssignee().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks a collateral.
     */
    public CheckCollateralEligibilityResponse checkCollateralEligibility(CheckCollateralEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckCollateralEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = BrowseWorkOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseWorkOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getCreatedAt())
                        .setOwnerId(request.getCollateral().getOwner().getUserId())
                        .setRegion(request.getCollateral().getAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.maintenance.v1.WorkOrderQueryService");
        var selected0 = WorkOrderQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.browseWorkOrders(outbound0);

        var outbound1 = NormalizeVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeVenue(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckCollateralEligibility", request.getCollateral().getCollateralId());
        return CheckCollateralEligibilityResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getVenue().getAudit().getCreatedBy())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(request.getCollateral().getOwner())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a collateral.
     */
    public NormalizeCollateralResponse normalizeCollateral(NormalizeCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = ListFiscalPeriodVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getCreatedAt())
                        .setOwnerId(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getComponentId())
                        .setRegion(switch (request.getCollateral().getLocatorCase()) {
                            case ADDRESS -> request.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFiscalPeriodVersions(outbound0);

        var outbound1 = CalculateClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateClaim(outbound1);

        var outbound2 = RecalculateUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateUnderwritingsRequest, RecalculateUnderwritingsResponse> port2 = RpcPort.adapt(insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateUnderwritings);
        var reply2 = port2.exchange(outbound2);

        var builder3 = ScoreStandingOrderRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setStandingOrder(StandingOrder.newBuilder()
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .setName(reply1.getClaim().getRequester().getName())
                                .build())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreStandingOrder(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeCollateral", request.getCollateral().getCollateralId());
        return NormalizeCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getClaim().getAudit().getCreatedAt())
                                .setUpdatedBy(reply3.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setName(request.getCollateral().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a collateral.
     */
    public ExplainCollateralDecisionResponse explainCollateralDecision(ExplainCollateralDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCollateralDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = EstimateCollateralRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getCollateral().getAddress().getLocality())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.CollateralCalculationService");
        var serviceName0 = "acme.lending.v1" + "." + "CollateralCalculationService";
        var methodName0 = "Est" + "imateCollateral";
        var descriptor0 = MethodDescriptor.<EstimateCollateralRequest, EstimateCollateralResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateCollateralRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateCollateralResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = EstimateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateRepaymentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getCollateral().getAddress().getRegion())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.RepaymentCalculationService");
        var serviceName1 = "acme.lending.v1" + "." + "RepaymentCalculationService";
        var methodName1 = "Est" + "imateRepayment";
        var descriptor1 = MethodDescriptor.<EstimateRepaymentRequest, EstimateRepaymentResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateRepaymentRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateRepaymentResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = LintLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getCollateral().getAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<LintLoanApplicationsRequest, LintLoanApplicationsResponse> operation2 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintLoanApplications;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCollateralDecision", request.getCollateral().getCollateralId());
        return ExplainCollateralDecisionResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setCapacity(request.getCollateral().getCapacity())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getRepayment().getOrganizer().getDisplayName())
                                .setEmail(reply2.getLoanApplication().getCounterparty().getEmail())
                                .setUserId(reply1.getRepayment().getOrganizer().getUserId())
                                .build())
                        .setStatus(reply0.getCollateral().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a collateral.
     */
    public ScoreCollateralResponse scoreCollateral(ScoreCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setRecipient(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .setRegion(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .setDimensions(CollateralDimensions.newBuilder()
                                .setUnit(request.getCollateral().getDimensions().getUnit())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCollateral(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreCollateral", request.getCollateral().getCollateralId());
        return ScoreCollateralResponse.newBuilder()
                .setBreakdown(ScoreCollateralResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .putComponents("primary", CollateralComponent.newBuilder()
                                .setQuantity(reply0.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getCollateral().getOwner().getUserId())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lints a collateral.
     */
    public LintCollateralsResponse lintCollaterals(LintCollateralsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintCollaterals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = (SummarizeClaimsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeClaimsRequest.Details.newBuilder()
                        .putAllFilters(request.getCollateral().getSpecsMap())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeClaims(outbound0);

        var outbound1 = SendAuditEventTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendAuditEventTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            SendAuditEventTestNoticeResponse fetch(SendAuditEventTestNoticeRequest input) {
                return auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendAuditEventTestNotice(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = EstimateSponsorRequest.newBuilder()
                .setContext(request.getContext())
                .setSponsor(Sponsor.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCollateral().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .setNationalId(SponsorNationalId.newBuilder()
                                .setNumber(reply1.getAuditEvent().getSubject().getNumber())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = sponsorshipsSponsorCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateSponsor(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintCollaterals", request.getCollateral().getCollateralId());
        return LintCollateralsResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .setName(request.getCollateral().getName())
                        .build())
                .setOutcome(LintCollateralsResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a collateral.
     */
    public GetCollateralRulesResponse getCollateralRules(GetCollateralRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollateralRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "GetCollateralRules");
        return GetCollateralRulesResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .build();
    }
}
