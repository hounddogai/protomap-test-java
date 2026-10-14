package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.customers.v1.CompareConsentRecordOptionsRequest;
import com.acme.customers.v1.CompareConsentRecordOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.SimulateCustomerRequest;
import com.acme.customers.v1.SimulateCustomerResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanApplicationWorkflowRequest;
import com.acme.lending.v1.PauseLoanApplicationWorkflowResponse;
import com.acme.ratelimiting.v1.CalculateRateLimitRequest;
import com.acme.ratelimiting.v1.CalculateRateLimitResponse;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitBlockAction;
import com.acme.ratelimiting.v1.RateLimitCalculationServiceGrpc;
import com.acme.treasury.v1.ActivateLiquidityForecastRequest;
import com.acme.treasury.v1.ActivateLiquidityForecastResponse;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireLiquidityForecastRequest;
import com.acme.treasury.v1.ExpireLiquidityForecastResponse;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleRequest;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastBankSettlement;
import com.acme.treasury.v1.LiquidityForecastReportingServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.ReinstateLiquidityForecastRequest;
import com.acme.treasury.v1.ReinstateLiquidityForecastResponse;
import com.acme.treasury.v1.RenewLiquidityForecastRequest;
import com.acme.treasury.v1.RenewLiquidityForecastResponse;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import com.acme.treasury.v1.SuspendLiquidityForecastRequest;
import com.acme.treasury.v1.SuspendLiquidityForecastResponse;
import com.acme.treasury.v1.TerminateLiquidityForecastRequest;
import com.acme.treasury.v1.TerminateLiquidityForecastResponse;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a liquidity forecast between the states of its lifecycle. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class LiquidityForecastLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastLifecycleApplication.class);

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final RenewalWorkflowServiceGrpc.RenewalWorkflowServiceFutureStub contractsRenewalWorkflowServiceFuture;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final RateLimitCalculationServiceGrpc.RateLimitCalculationServiceBlockingStub ratelimitingRateLimitCalculationServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final LiquidityForecastReportingServiceGrpc.LiquidityForecastReportingServiceBlockingStub treasuryLiquidityForecastReportingServiceBlocking;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public LiquidityForecastLifecycleApplication(
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            RenewalWorkflowServiceGrpc.RenewalWorkflowServiceFutureStub contractsRenewalWorkflowServiceFuture,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            RateLimitCalculationServiceGrpc.RateLimitCalculationServiceBlockingStub ratelimitingRateLimitCalculationServiceBlocking,
            TreasuryRepository repository,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            LiquidityForecastReportingServiceGrpc.LiquidityForecastReportingServiceBlockingStub treasuryLiquidityForecastReportingServiceBlocking,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.contractsRenewalWorkflowServiceFuture = contractsRenewalWorkflowServiceFuture;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.ratelimitingRateLimitCalculationServiceBlocking = ratelimitingRateLimitCalculationServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryLiquidityForecastReportingServiceBlocking = treasuryLiquidityForecastReportingServiceBlocking;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Activates a liquidity forecast.
     */
    public ActivateLiquidityForecastResponse activateLiquidityForecast(ActivateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "ActivateLiquidityForecast");

        var outbound0 = LintAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintAuditTrailsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.audit.v1.AuditTrailValidationService/LintAuditTrails", outbound0);
        var reply0 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditTrails(command0.payload());

        var outbound1 = (CompareConsentRecordOptionsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareConsentRecordOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getAuditTrail().getBillingAddress().getPostalCode())
                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareConsentRecordOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateLiquidityForecast", request.getDetails().getReasonCode());
        return ActivateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(ActivateLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getConsentRecord().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Suspends a liquidity forecast.
     */
    public SuspendLiquidityForecastResponse suspendLiquidityForecast(SuspendLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "SuspendLiquidityForecast");

        var outbound0 = StreamLiquidityForecastMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = treasuryLiquidityForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLiquidityForecastMetrics(outbound0);
        var reply0 = StreamLiquidityForecastMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendLiquidityForecast", request.getDetails().getReasonCode());
        return SuspendLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(SuspendLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Reinstates a liquidity forecast.
     */
    public ReinstateLiquidityForecastResponse reinstateLiquidityForecast(ReinstateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "ReinstateLiquidityForecast");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateRateLimitRequest.newBuilder()
                .setContext(request.getContext())
                .setRateLimit(RateLimit.newBuilder()
                        .setBlock(RateLimitBlockAction.newBuilder()
                                .setNotifyEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build()), CalculateRateLimitRequest.class);
        var reply0 = ratelimitingRateLimitCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateRateLimit(outbound0);

        var outbound1 = PauseLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            PauseLoanApplicationWorkflowResponse fetch(PauseLoanApplicationWorkflowRequest input) {
                return lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseLoanApplicationWorkflow(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = SimulateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRateLimit().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getRateLimit().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            SimulateCustomerResponse fetch(SimulateCustomerRequest input) {
                return customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomer(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = ResumeRenewalWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResumeRenewalWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply2.getCustomer().getContact().getEmail())
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotifyContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending3 = contractsRenewalWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resumeRenewalWorkflow(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateLiquidityForecast", request.getDetails().getReasonCode());
        return ReinstateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getLoanApplication().getBillingAddress().getLocality())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setOutcome(ReinstateLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a liquidity forecast.
     */
    public TerminateLiquidityForecastResponse terminateLiquidityForecast(TerminateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "TerminateLiquidityForecast");

        var outbound0 = FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        RpcPort<FindAuditEventsRequest, FindAuditEventsResponse> port0 = RpcPort.adapt(auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findAuditEvents);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = UpdateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDetails().getEffectiveAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<UpdateVenueRequest, UpdateVenueResponse>() {
            @Override
            public UpdateVenueResponse apply(UpdateVenueRequest input) {
                return venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateVenue(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateLiquidityForecast", request.getDetails().getReasonCode());
        return TerminateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getVenue().getAddress().getRecipient())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(TerminateLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a liquidity forecast.
     */
    public RenewLiquidityForecastResponse renewLiquidityForecast(RenewLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "RenewLiquidityForecast");

        var outbound0 = BeanCopies.copy(RevertJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertJournalEntryRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build(), RevertJournalEntryRequest.class);
        var reply0 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertJournalEntry(outbound0);

        var outbound1 = CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotifyContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.collections.v1.PaymentPromiseApprovalService/CommentPaymentPromiseApproval", outbound1);
        var reply1 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPaymentPromiseApproval(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewLiquidityForecast", request.getDetails().getReasonCode());
        return RenewLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getJournalEntry().getCounterparty().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(RenewLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a liquidity forecast.
     */
    public ExpireLiquidityForecastResponse expireLiquidityForecast(ExpireLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "ExpireLiquidityForecast");

        var outbound0 = CountAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getLiquidityForecastId())
                        .build())
                .build();
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAuditEvents(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireLiquidityForecast", request.getDetails().getReasonCode());
        return ExpireLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(ExpireLiquidityForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastLifecycleResponse getLiquidityForecastLifecycle(GetLiquidityForecastLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastLifecycle");

        var outbound0 = PreviewBankTransferRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewBankTransferRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBankTransfer(value)).orElseThrow();
        return GetLiquidityForecastLifecycleResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a liquidity forecast.
     */
    public ScheduleLiquidityForecastExpiryResponse scheduleLiquidityForecastExpiry(ScheduleLiquidityForecastExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleLiquidityForecastExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = PullLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullLiquidityForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBank(LiquidityForecastBankSettlement.newBuilder()
                                .setIban(request.getLiquidityForecast().getBank().getIban())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullLiquidityForecasts(value)).orElseThrow();

        var outbound1 = ApproveCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(ApproveCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.CoverageApprovalService");
        var serviceName1 = "acme.insurance.v1" + "." + "CoverageApprovalService";
        var methodName1 = "App" + "roveCoverage";
        var descriptor1 = MethodDescriptor.<ApproveCoverageRequest, ApproveCoverageResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ApproveCoverageRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ApproveCoverageResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleLiquidityForecastExpiry", request.getLiquidityForecast().getLiquidityForecastId());
        return ScheduleLiquidityForecastExpiryResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(reply0.getLiquidityForecast().getCounterparty())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .build();
    }
}
