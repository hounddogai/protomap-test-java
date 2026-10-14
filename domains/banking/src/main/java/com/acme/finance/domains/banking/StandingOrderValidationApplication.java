package com.acme.finance.domains.banking;

import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetStandingOrderRulesRequest;
import com.acme.banking.v1.GetStandingOrderRulesResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.ExportClaimsRequest;
import com.acme.insurance.v1.ExportClaimsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.shows.v1.CompareShowOptionsRequest;
import com.acme.shows.v1.CompareShowOptionsResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.shows.v1.ShowDailyRule;
import com.acme.shows.v1.ShowParticipant;
import com.acme.venues.v1.NormalizeVenueRequest;
import com.acme.venues.v1.NormalizeVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks standing orders against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StandingOrderValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(StandingOrderValidationApplication.class);

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final StandingOrderValidationRequestMapper mapper;

    private final BankingRepository repository;

    private final Executor rpcExecutor;

    private final ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public StandingOrderValidationApplication(
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            StandingOrderValidationRequestMapper mapper,
            BankingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.showsShowCalculationServiceBlocking = showsShowCalculationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Validates a standing order.
     */
    public ValidateStandingOrderResponse validateStandingOrder(ValidateStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = BeanCopies.copy(QuoteMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getStandingOrder().getLocation().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build(), QuoteMandateRequest.class);
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteMandate(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateStandingOrder", request.getStandingOrder().getStandingOrderId());
        return ValidateStandingOrderResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .putAllMetadata(request.getStandingOrder().getMetadataMap())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Previews a standing order.
     */
    public PreviewStandingOrderResponse previewStandingOrder(PreviewStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = ExplainUnderwritingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainUnderwritingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStandingOrder().getAudit().getCreatedAt())
                                .setCreatedBy(request.getStandingOrder().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getStandingOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUnderwritingDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = JsonCodec.roundTrip(CompareMandateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getUnderwriting().getAudit().getCreatedAt())
                                .setEtag(reply0.getUnderwriting().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getStandingOrder().getLocation().getPostalCode())
                                .setRecipient(request.getStandingOrder().getLocation().getRecipient())
                                .build())
                        .build())
                .build(), CompareMandateOptionsRequest.class);
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareMandateOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewStandingOrder", request.getStandingOrder().getStandingOrderId());
        return PreviewStandingOrderResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getUnderwriting().getRequester().getEmail())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setStatus(request.getStandingOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a standing order.
     */
    public CheckStandingOrderEligibilityResponse checkStandingOrderEligibility(CheckStandingOrderEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckStandingOrderEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var futures0 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> ListUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUsersRequest.Filter.newBuilder()
                        .setOwnerId(element.getParticipantId())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUsers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ListUsersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExplainPremiumCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPremiumCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStandingOrder().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPremiumCalculation(outbound1);

        var futures2 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> NormalizeVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeVenue(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? NormalizeVenueResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckStandingOrderEligibility", request.getStandingOrder().getStandingOrderId());
        return CheckStandingOrderEligibilityResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPremium().getAudit().getCreatedBy())
                                .build())
                        .setOrganizer(request.getStandingOrder().getOrganizer())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Normalizes a standing order.
     */
    public NormalizeStandingOrderResponse normalizeStandingOrder(NormalizeStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var futures0 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintJournalEntriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setName(element.getContact().getName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintJournalEntries(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? LintJournalEntriesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getJournalEntry().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getStandingOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.identity.v1.UserService");
        var selected1 = UserServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.updateUser(outbound1);

        var outbound2 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.ledger.v1.PostingBatchNotificationService");
        var serviceName2 = "acme.ledger.v1" + "." + "PostingBatchNotificationService";
        var methodName2 = "Sub" + "scribePostingBatchUpdates";
        var descriptor2 = MethodDescriptor.<SubscribePostingBatchUpdatesRequest, SubscribePostingBatchUpdatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(SubscribePostingBatchUpdatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubscribePostingBatchUpdatesResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getStandingOrder().getLocation().getRegion())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.metrics.v1.MetricSeriesService");
        var selected3 = MetricSeriesServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.updateMetricSeries(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeStandingOrder", request.getStandingOrder().getStandingOrderId());
        return NormalizeStandingOrderResponse.newBuilder()
                .setOutcome(NormalizeStandingOrderResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply2.getPostingBatch().getBillingAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getUser().getContact().getEmail())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setStatus(request.getStandingOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a standing order.
     */
    public ExplainStandingOrderDecisionResponse explainStandingOrderDecision(ExplainStandingOrderDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainStandingOrderDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = PreviewUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingValidationService");
        var selected0 = UnderwritingValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.previewUnderwriting(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainStandingOrderDecision", request.getStandingOrder().getStandingOrderId());
        return ExplainStandingOrderDecisionResponse.newBuilder()
                .setOutcome(ExplainStandingOrderDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUnderwriting().getAudit().getEtag())
                                .build())
                        .setLocation(request.getStandingOrder().getLocation())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Scores a standing order.
     */
    public ScoreStandingOrderResponse scoreStandingOrder(ScoreStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = RecalculateMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStandingOrder().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound0), rpcExecutor);

        var outbound1 = CompareShowOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setShow(Show.newBuilder()
                        .setDaily(ShowDailyRule.newBuilder()
                                .setInterval(request.getStandingOrder().getDaily().getInterval())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .addParticipants(ShowParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> showsShowCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareShowOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreStandingOrder", request.getStandingOrder().getStandingOrderId());
        return ScoreStandingOrderResponse.newBuilder()
                .setBreakdown(ScoreStandingOrderResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getShow().getAudit().getUpdatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply1.getShow().getOrganizer().getDisplayName())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Lints a standing order.
     */
    public LintStandingOrdersResponse lintStandingOrders(LintStandingOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintStandingOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = QuoteMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getStandingOrder().getLocation().getCountryCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.MandateCalculationService");
        var selected0 = MandateCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quoteMandate(outbound0);

        var outbound1 = ExportClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportClaimsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getMandate().getDueAt())
                        .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportClaims(outbound1);
        var reply1 = ExportClaimsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        List<UpdateUserResponse> results2 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateUser(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? UpdateUserResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateMandateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getUser().getPrimaryAddress().getCountryCode())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getMandate().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintStandingOrders", request.getStandingOrder().getStandingOrderId());
        return LintStandingOrdersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .putAllMetadata(reply0.getMandate().getMetadataMap())
                        .setOrganizer(request.getStandingOrder().getOrganizer())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Returns a standing order.
     */
    public GetStandingOrderRulesResponse getStandingOrderRules(GetStandingOrderRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetStandingOrderRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StandingOrder", request.getStandingOrderId(), "GetStandingOrderRules");

        var outbound0 = mapper.getStandingOrderRulesStage0(request);
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound0);
        return GetStandingOrderRulesResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }
}
