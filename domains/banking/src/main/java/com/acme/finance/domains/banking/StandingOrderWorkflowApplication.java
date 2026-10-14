package com.acme.finance.domains.banking;

import com.acme.banking.v1.AdvanceStandingOrderWorkflowRequest;
import com.acme.banking.v1.AdvanceStandingOrderWorkflowResponse;
import com.acme.banking.v1.CancelStandingOrderWorkflowRequest;
import com.acme.banking.v1.CancelStandingOrderWorkflowResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.InspectStandingOrderWorkflowRequest;
import com.acme.banking.v1.InspectStandingOrderWorkflowResponse;
import com.acme.banking.v1.ListStandingOrderWorkflowsRequest;
import com.acme.banking.v1.ListStandingOrderWorkflowsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.ResumeStandingOrderWorkflowRequest;
import com.acme.banking.v1.ResumeStandingOrderWorkflowResponse;
import com.acme.banking.v1.RetryStandingOrderWorkflowRequest;
import com.acme.banking.v1.RetryStandingOrderWorkflowResponse;
import com.acme.banking.v1.RetryStatementWorkflowRequest;
import com.acme.banking.v1.RetryStatementWorkflowResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.SignalStandingOrderWorkflowRequest;
import com.acme.banking.v1.SignalStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StartStandingOrderWorkflowRequest;
import com.acme.banking.v1.StartStandingOrderWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.employees.v1.CloneEmployeeRequest;
import com.acme.employees.v1.CloneEmployeeResponse;
import com.acme.employees.v1.Employee;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.CancelPolicyWorkflowRequest;
import com.acme.insurance.v1.CancelPolicyWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.search.v1.ScoreSearchIndexRequest;
import com.acme.search.v1.ScoreSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.seating.v1.CreateSeatMapRequest;
import com.acme.seating.v1.CreateSeatMapResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a standing order from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class StandingOrderWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(StandingOrderWorkflowApplication.class);

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking;

    private final ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final BankingRepository repository;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SeatMapServiceGrpc.SeatMapServiceBlockingStub seatingSeatMapServiceBlocking;

    public StandingOrderWorkflowApplication(
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking,
            ChannelRegistry channels,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking,
            ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            BankingRepository repository,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SeatMapServiceGrpc.SeatMapServiceBlockingStub seatingSeatMapServiceBlocking) {
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.bankingStatementWorkflowServiceBlocking = bankingStatementWorkflowServiceBlocking;
        this.channels = channels;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.employeesEmployeeServiceBlocking = employeesEmployeeServiceBlocking;
        this.insuranceClaimLifecycleServiceBlocking = insuranceClaimLifecycleServiceBlocking;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.seatingSeatMapServiceBlocking = seatingSeatMapServiceBlocking;
    }

    /**
     * Starts a standing order.
     */
    public StartStandingOrderWorkflowResponse startStandingOrderWorkflow(StartStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = BeanCopies.copy(ScoreSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreSearchIndexRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getStandingOrder().getLocation().getCountryCode())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getStandingOrder().getLocation().getRegion())
                                .build())
                        .build())
                .build(), ScoreSearchIndexRequest.class);
        var reply0 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSearchIndex(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return StartStandingOrderWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setStandingOrderId(request.getStandingOrderId())
                        .setStatus(request.getStandingOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a standing order.
     */
    public AdvanceStandingOrderWorkflowResponse advanceStandingOrderWorkflow(AdvanceStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        List<SyncPremiumsResponse> results0 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> SyncPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStandingOrder().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getStandingOrder().getLocation().getRecipient())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncPremiums(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SyncPremiumsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CreateSeatMapRequest.newBuilder()
                .setContext(request.getContext())
                .setSeatMap(SeatMap.newBuilder()
                        .addChildren(SeatMap.newBuilder()
                                .addChildren(SeatMap.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(reply0.getPremium().getBillingAddress().getRecipient())
                                                .setRegion(request.getStandingOrder().getLocation().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(seatingSeatMapServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSeatMap(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return AdvanceStandingOrderWorkflowResponse.newBuilder()
                .setOutcome(AdvanceStandingOrderWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .putAllMetadata(request.getStandingOrder().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply1.getSeatMap().getManager().getUserId())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Cancels a standing order.
     */
    public CancelStandingOrderWorkflowResponse cancelStandingOrderWorkflow(CancelStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var results0 = new ArrayList<ApprovePremiumResponse>();
        for (var element : request.getStandingOrder().getParticipantsList()) {
            ApprovePremiumRequest itemRequest = ApprovePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApprovePremiumRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getStandingOrder().getLocation().getLocality())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
            results0.add(insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePremium(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ApprovePremiumResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CancelPolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPremium().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelPolicyWorkflow(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return CancelStandingOrderWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(reply1.getPolicy().getContact().getName())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setTitle(request.getStandingOrder().getTitle())
                        .build())
                .build();
    }

    /**
     * Retrys a standing order.
     */
    public RetryStandingOrderWorkflowResponse retryStandingOrderWorkflow(RetryStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = ScheduleClaimExpiryRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getStandingOrder().getOrganizer().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getStandingOrder().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ScheduleClaimExpiryRequest, ScheduleClaimExpiryResponse> port0 = RpcPort.adapt(insuranceClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleClaimExpiry);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<DelegateStandingOrderApprovalResponse>();
        for (var element : request.getStandingOrder().getParticipantsList()) {
            DelegateStandingOrderApprovalRequest itemRequest = DelegateStandingOrderApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(element.getContact().getEmail())
                                        .setName(element.getContact().getName())
                                        .setPhone(element.getContact().getPhone())
                                        .build())
                                .setParticipantId(element.getParticipantId())
                                .build())
                        .build())
                .build();
            results1.add(bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateStandingOrderApproval(itemRequest));
        }
        var reply1 = results1.isEmpty() ? DelegateStandingOrderApprovalResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = RetryStatementWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getStandingOrder().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var reply2 = bankingStatementWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryStatementWorkflow(outbound2);

        var outbound3 = ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getStandingOrder().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.venues.v1.VenueValidationService");
        var selected3 = VenueValidationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.validateVenue(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return RetryStandingOrderWorkflowResponse.newBuilder()
                .setOutcome(RetryStandingOrderWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getVenue().getAudit().getCreatedAt())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setWindow(request.getStandingOrder().getWindow())
                        .build())
                .build();
    }

    /**
     * Pauses a standing order.
     */
    public PauseStandingOrderWorkflowResponse pauseStandingOrderWorkflow(PauseStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = EscalateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateMandateRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getStandingOrder().getLocation().getCountryCode())
                                .setLocality(request.getStandingOrder().getLocation().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateMandate(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getStandingOrder().getAudit().getEtag())
                                .setUpdatedBy(request.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        Function<LintCustomersRequest, LintCustomersResponse> operation1 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintCustomers;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = (ValidateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getCustomer().getPrimaryAddress().getRecipient())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDocument(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(CloneEmployeeRequest.newBuilder()
                .setContext(request.getContext())
                .setEmployee(Employee.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getCustomer().getContact().getPhone())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getCustomer().getPrimaryAddress().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(reply1.getCustomer().getRelationshipManager().getUserId())
                                .build())
                        .build())
                .build()), CloneEmployeeRequest.class);
        var reply3 = employeesEmployeeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneEmployee(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return PauseStandingOrderWorkflowResponse.newBuilder()
                .setOutcome(PauseStandingOrderWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .setRecipient(reply3.getEmployee().getPrimaryAddress().getRecipient())
                                .build())
                        .setOrganizer(request.getStandingOrder().getOrganizer())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply3.getEmployee().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Resumes a standing order.
     */
    public ResumeStandingOrderWorkflowResponse resumeStandingOrderWorkflow(ResumeStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = (ResumeClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResumeClaimWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeClaimWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return ResumeStandingOrderWorkflowResponse.newBuilder()
                .setOutcome(ResumeStandingOrderWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(request.getStandingOrder().getLocation())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Returns a standing order.
     */
    public InspectStandingOrderWorkflowResponse inspectStandingOrderWorkflow(InspectStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StandingOrder", request.getStandingOrderId(), "InspectStandingOrderWorkflow");
        return InspectStandingOrderWorkflowResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Lists standing orders a page at a time.
     */
    public ListStandingOrderWorkflowsResponse listStandingOrderWorkflows(ListStandingOrderWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListStandingOrderWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StandingOrder", request.getContext().getTraceId(), "ListStandingOrderWorkflows");

        var outbound0 = JsonCodec.roundTrip(DelegatePremiumApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), DelegatePremiumApprovalRequest.class);
        var reply0 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegatePremiumApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListStandingOrderWorkflows", request.getPage().getPageToken());
        return ListStandingOrderWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .addStandingOrders(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a standing order.
     */
    public SignalStandingOrderWorkflowResponse signalStandingOrderWorkflow(SignalStandingOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalStandingOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var builder0 = ScoreMandateRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setMandate(Mandate.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(request.getStandingOrder().getAudit().getEtag())
                        .build())
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRecipient(request.getStandingOrder().getLocation().getRecipient())
                        .build())
                .setCounterparty(CustomerRef.newBuilder()
                        .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMandate(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalStandingOrderWorkflow", request.getStandingOrder().getStandingOrderId());
        return SignalStandingOrderWorkflowResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setStatus(request.getStandingOrder().getStatus())
                        .build())
                .build();
    }
}
