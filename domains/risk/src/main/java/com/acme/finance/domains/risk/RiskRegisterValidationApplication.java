package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.ScoreRiskRegisterRequest;
import com.acme.risk.v1.ScoreRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.shows.v1.FindShowsRequest;
import com.acme.shows.v1.FindShowsResponse;
import com.acme.shows.v1.ShowQueryServiceGrpc;
import com.acme.telematics.v1.BrowseTelemetryReadingsRequest;
import com.acme.telematics.v1.BrowseTelemetryReadingsResponse;
import com.acme.telematics.v1.TelemetryReadingQueryServiceGrpc;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks risk registers against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskRegisterValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskRegisterValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final RiskRegisterValidationRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final RiskRepository repository;

    private final ShowQueryServiceGrpc.ShowQueryServiceBlockingStub showsShowQueryServiceBlocking;

    private final TelemetryReadingQueryServiceGrpc.TelemetryReadingQueryServiceBlockingStub telematicsTelemetryReadingQueryServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public RiskRegisterValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            RiskRegisterValidationRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            RiskRepository repository,
            ShowQueryServiceGrpc.ShowQueryServiceBlockingStub showsShowQueryServiceBlocking,
            TelemetryReadingQueryServiceGrpc.TelemetryReadingQueryServiceBlockingStub telematicsTelemetryReadingQueryServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.showsShowQueryServiceBlocking = showsShowQueryServiceBlocking;
        this.telematicsTelemetryReadingQueryServiceBlocking = telematicsTelemetryReadingQueryServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Validates a risk register.
     */
    public ValidateRiskRegisterResponse validateRiskRegister(ValidateRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = CheckUserEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.identity.v1.UserValidationService/CheckUserEligibility", outbound0);
        var reply0 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkUserEligibility(command0.payload());

        var outbound1 = StreamRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyExport(outbound1);
        var reply1 = StreamRetentionPolicyExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRiskRegister().getAudit().getCreatedAt())
                                .setCreatedBy(request.getRiskRegister().getAudit().getCreatedBy())
                                .setEtag(request.getRiskRegister().getAudit().getEtag())
                                .setUpdatedAt(request.getRiskRegister().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return ValidateRiskRegisterResponse.newBuilder()
                .setOutcome(ValidateRiskRegisterResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getUser().getAudit().getUpdatedAt())
                                .build())
                        .setName(request.getRiskRegister().getName())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Previews a risk register.
     */
    public PreviewRiskRegisterResponse previewRiskRegister(PreviewRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = ScoreStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreStandingOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getRiskRegister().getLocation().getCountryCode())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreStandingOrder(outbound0);

        var outbound1 = JsonCodec.roundTrip(FindShowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindShowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getCreatedAt())
                        .setRegion(reply0.getStandingOrder().getLocation().getRegion())
                        .build())
                .build(), FindShowsRequest.class);
        var reply1 = showsShowQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findShows(outbound1);

        var outbound2 = ScoreRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            ScoreRepaymentResponse fetch(ScoreRepaymentRequest input) {
                return lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRepayment(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return PreviewRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(reply0.getStandingOrder().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply2.getRepayment().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setUserId(reply2.getRepayment().getOrganizer().getUserId())
                                        .build())
                                .build())
                        .setDepth(request.getRiskRegister().getDepth())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Checks a risk register.
     */
    public CheckRiskRegisterEligibilityResponse checkRiskRegisterEligibility(CheckRiskRegisterEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRiskRegisterEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = BeanCopies.copy(CheckRepaymentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRepaymentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getRiskRegister().getManager().getEmail())
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build(), CheckRepaymentEligibilityRequest.class);
        var reply0 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRepaymentEligibility(outbound0);

        var outbound1 = StreamRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyExportRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getRepayment().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyExport(outbound1);
        var reply1 = StreamRetentionPolicyExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRiskRegisterEligibility", request.getRiskRegister().getRiskRegisterId());
        return CheckRiskRegisterEligibilityResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply0.getRepayment().getAudit().getEtag())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getRepayment().getOrganizer().getDisplayName())
                                        .build())
                                .build())
                        .setParentId(request.getRiskRegister().getParentId())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Normalizes a risk register.
     */
    public NormalizeRiskRegisterResponse normalizeRiskRegister(NormalizeRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = BeanCopies.copy(PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRiskRegister().getAudit().getEtag())
                                .setUpdatedAt(request.getRiskRegister().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build(), PreviewMandateRequest.class);
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(outbound0);

        var outbound1 = mapper.normalizeRiskRegisterStage1(request);
        var reply1 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(outbound1);

        var outbound2 = RetryFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.ledger.v1.FiscalPeriodExportService");
        var serviceName2 = "acme.ledger.v1" + "." + "FiscalPeriodExportService";
        var methodName2 = "Ret" + "ryFiscalPeriodExport";
        var descriptor2 = MethodDescriptor.<RetryFiscalPeriodExportRequest, RetryFiscalPeriodExportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(RetryFiscalPeriodExportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RetryFiscalPeriodExportResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = JsonCodec.roundTrip(ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRiskRegister().getAudit().getCreatedBy())
                                .setEtag(reply1.getStandingOrder().getAudit().getEtag())
                                .setUpdatedBy(reply2.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getRiskRegister().getManager().getUserId())
                                .build())
                        .build())
                .build(), ValidateVenueRequest.class);
        var reply3 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return NormalizeRiskRegisterResponse.newBuilder()
                .setOutcome(NormalizeRiskRegisterResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply2.getFiscalPeriod().getLocation().getLinesList())
                                .setLocality(reply1.getStandingOrder().getLocation().getLocality())
                                .build())
                        .setParentId(request.getRiskRegister().getParentId())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Explains a risk register.
     */
    public ExplainRiskRegisterDecisionResponse explainRiskRegisterDecision(ExplainRiskRegisterDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRiskRegisterDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var builder0 = ValidateStandingOrderRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setStandingOrder(StandingOrder.newBuilder()
                .setOrganizer(UserRef.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .build())
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(outbound0);

        var outbound1 = SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getRiskRegister().getManager().getUserId())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        Function<SearchNotificationsRequest, SearchNotificationsResponse> operation1 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchNotifications;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = BrowseTelemetryReadingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseTelemetryReadingsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getStandingOrder().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getStandingOrder().getOrganizer().getUserId())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(telematicsTelemetryReadingQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseTelemetryReadings(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRiskRegisterDecision", request.getRiskRegister().getRiskRegisterId());
        return ExplainRiskRegisterDecisionResponse.newBuilder()
                .setOutcome(ExplainRiskRegisterDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setManager(request.getRiskRegister().getManager())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Scores a risk register.
     */
    public ScoreRiskRegisterResponse scoreRiskRegister(ScoreRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = CreateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getRiskRegister().getLocation().getLocality())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .build();
        Function<CreateLoanApplicationRequest, CreateLoanApplicationResponse> operation0 = lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createLoanApplication;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCard(MandateCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getLoanApplication().getSettlementCase()) {
                                    case CARD -> reply0.getLoanApplication().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return ScoreRiskRegisterResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(reply0.getLoanApplication().getAudit().getUpdatedBy())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getMandate().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .setManager(request.getRiskRegister().getManager())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Lints a risk register.
     */
    public LintRiskRegistersResponse lintRiskRegisters(LintRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = NormalizeStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeStandingOrderRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getRiskRegister().getLocation().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeStandingOrder(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintRiskRegisters", request.getRiskRegister().getRiskRegisterId());
        return LintRiskRegistersResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .putAllAttributes(request.getRiskRegister().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Returns a risk register.
     */
    public GetRiskRegisterRulesResponse getRiskRegisterRules(GetRiskRegisterRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRiskRegisterRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "GetRiskRegisterRules");

        var outbound0 = ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(value));
        return GetRiskRegisterRulesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }
}
