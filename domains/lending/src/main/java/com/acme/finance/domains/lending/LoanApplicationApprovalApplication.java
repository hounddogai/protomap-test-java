package com.acme.finance.domains.lending;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerDecisionRequest;
import com.acme.customers.v1.ExplainCustomerDecisionResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CommentLoanApplicationApprovalRequest;
import com.acme.lending.v1.CommentLoanApplicationApprovalResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.RejectLoanApplicationRequest;
import com.acme.lending.v1.RejectLoanApplicationResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lending.v1.SubmitLoanApplicationRequest;
import com.acme.lending.v1.SubmitLoanApplicationResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.notifications.v1.ExportNotificationsRequest;
import com.acme.notifications.v1.ExportNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.google.protobuf.Any;
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
 * Routes loan applications through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LoanApplicationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationApprovalApplication.class);

    private final ChannelRegistry channels;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final LoanApplicationApprovalRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    public LoanApplicationApprovalApplication(
            ChannelRegistry channels,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            LoanApplicationApprovalRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a loan application.
     */
    public SubmitLoanApplicationResponse submitLoanApplication(SubmitLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = CalculateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.CollateralCalculationService");
        var selected0 = CollateralCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.calculateCollateral(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return SubmitLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollateral().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getCollateral().getOwner().getEmail())
                                .build())
                        .setIssuedAt(request.getLoanApplication().getIssuedAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }

    /**
     * Approves a loan application.
     */
    public ApproveLoanApplicationResponse approveLoanApplication(ApproveLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = mapper.approveLoanApplicationStage0(request);
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLoanApplication(outbound0);

        var outbound1 = ResolveHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<ResolveHouseholdsRequest, ResolveHouseholdsResponse>() {
            @Override
            public ResolveHouseholdsResponse apply(ResolveHouseholdsRequest input) {
                return customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveHouseholds(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return ApproveLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(request.getLoanApplication().getCounterparty())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setOutcome(ApproveLoanApplicationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a loan application.
     */
    public RejectLoanApplicationResponse rejectLoanApplication(RejectLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = (ExplainCustomerDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerDecision(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return RejectLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(request.getLoanApplication().getCounterparty())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists loan applications a page at a time.
     */
    public ListPendingLoanApplicationsResponse listPendingLoanApplications(ListPendingLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "ListPendingLoanApplications");

        var outbound0 = JsonCodec.roundTrip(ValidateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), ValidateLoanApplicationRequest.class);
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLoanApplication(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingLoanApplications", request.getPage().getPageToken());
        return ListPendingLoanApplicationsResponse.newBuilder()
                .addLoanApplications(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a loan application.
     */
    public DelegateLoanApplicationApprovalResponse delegateLoanApplicationApproval(DelegateLoanApplicationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateLoanApplicationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var futures0 = request.getLoanApplication().getLinesList().stream()
                .map(element -> GenerateRepaymentReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateRepaymentReportRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateRepaymentReport(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? GenerateRepaymentReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = mapper.delegateLoanApplicationApprovalStage1(request);
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanApplicationCalculation(outbound1);

        var outbound2 = ResumeCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(ResumeCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeCoverageWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = ExportNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportNotificationsRequest.Filter.newBuilder()
                        .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream3 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportNotifications(outbound3);
        var reply3 = ExportNotificationsResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateLoanApplicationApproval", request.getLoanApplication().getLoanApplicationId());
        return DelegateLoanApplicationApprovalResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setNotes(reply1.getLoanApplication().getNotes())
                        .setNumber(request.getLoanApplication().getNumber())
                        .build())
                .setOutcome(DelegateLoanApplicationApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a loan application.
     */
    public RecallLoanApplicationResponse recallLoanApplication(RecallLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = QuoteConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<QuoteConsentRecordRequest, QuoteConsentRecordResponse> port0 = RpcPort.adapt(customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteConsentRecord);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return RecallLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setTotal(request.getLoanApplication().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a loan application.
     */
    public GetLoanApplicationApprovalStatusResponse getLoanApplicationApprovalStatus(GetLoanApplicationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanApplicationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "GetLoanApplicationApprovalStatus");

        var outbound0 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getLoanApplicationId())
                        .build())
                .build();
        Function<FindHouseholdsRequest, FindHouseholdsResponse> operation0 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findHouseholds;
        var reply0 = operation0.apply(outbound0);
        return GetLoanApplicationApprovalStatusResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a loan application.
     */
    public CommentLoanApplicationApprovalResponse commentLoanApplicationApproval(CommentLoanApplicationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentLoanApplicationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = StreamRepaymentMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRepaymentMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                        .build())
                .build();
        var stream0 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRepaymentMetrics(outbound0);
        var reply0 = StreamRepaymentMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = NormalizeLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getLoanApplication().getBillingAddress().getLinesList())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setHolderName(switch (request.getLoanApplication().getSettlementCase()) {
                                    case CARD -> request.getLoanApplication().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLoanApplication(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentLoanApplicationApproval", request.getLoanApplication().getLoanApplicationId());
        return CommentLoanApplicationApprovalResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(reply1.getLoanApplication().getCounterparty())
                        .setIssuedAt(request.getLoanApplication().getIssuedAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }
}
