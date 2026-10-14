package com.acme.finance.domains.expenses;

import com.acme.catalog.v1.FindProductsRequest;
import com.acme.catalog.v1.FindProductsResponse;
import com.acme.catalog.v1.ProductQueryServiceGrpc;
import com.acme.chat.v1.CalculateChatRoomRequest;
import com.acme.chat.v1.CalculateChatRoomResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomCalculationServiceGrpc;
import com.acme.chat.v1.ChatRoomComponent;
import com.acme.chat.v1.QuoteChatRoomRequest;
import com.acme.chat.v1.QuoteChatRoomResponse;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.employees.v1.EmployeeRef;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.DelegateExpenseReportApprovalRequest;
import com.acme.expenses.v1.DelegateExpenseReportApprovalResponse;
import com.acme.expenses.v1.EscalateExpenseReportRequest;
import com.acme.expenses.v1.EscalateExpenseReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListPendingExpenseReportsRequest;
import com.acme.expenses.v1.ListPendingExpenseReportsResponse;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RejectExpenseReportRequest;
import com.acme.expenses.v1.RejectExpenseReportResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.expenses.v1.SubmitExpenseReportRequest;
import com.acme.expenses.v1.SubmitExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CountLedgerAccountsRequest;
import com.acme.ledger.v1.CountLedgerAccountsResponse;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.maintenance.v1.CompareWorkOrderOptionsRequest;
import com.acme.maintenance.v1.CompareWorkOrderOptionsResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.media.v1.ExplainMediaAssetCalculationRequest;
import com.acme.media.v1.ExplainMediaAssetCalculationResponse;
import com.acme.media.v1.MediaAsset;
import com.acme.timekeeping.v1.CreateTimesheetRequest;
import com.acme.timekeeping.v1.CreateTimesheetResponse;
import com.acme.timekeeping.v1.Timesheet;
import com.acme.timekeeping.v1.TimesheetServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes expense reports through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExpenseReportApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportApprovalApplication.class);

    private final ProductQueryServiceGrpc.ProductQueryServiceFutureStub catalogProductQueryServiceFuture;

    private final ChannelRegistry channels;

    private final ChatRoomCalculationServiceGrpc.ChatRoomCalculationServiceBlockingStub chatChatRoomCalculationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final MileageClaimReportingServiceGrpc.MileageClaimReportingServiceFutureStub expensesMileageClaimReportingServiceFuture;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking;

    private final ExpensesRepository repository;

    private final TimesheetServiceGrpc.TimesheetServiceBlockingStub timekeepingTimesheetServiceBlocking;

    public ExpenseReportApprovalApplication(
            ProductQueryServiceGrpc.ProductQueryServiceFutureStub catalogProductQueryServiceFuture,
            ChannelRegistry channels,
            ChatRoomCalculationServiceGrpc.ChatRoomCalculationServiceBlockingStub chatChatRoomCalculationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            MileageClaimReportingServiceGrpc.MileageClaimReportingServiceFutureStub expensesMileageClaimReportingServiceFuture,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking,
            ExpensesRepository repository,
            TimesheetServiceGrpc.TimesheetServiceBlockingStub timekeepingTimesheetServiceBlocking) {
        this.catalogProductQueryServiceFuture = catalogProductQueryServiceFuture;
        this.channels = channels;
        this.chatChatRoomCalculationServiceBlocking = chatChatRoomCalculationServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.events = events;
        this.expensesExpensePolicyLifecycleServiceBlocking = expensesExpensePolicyLifecycleServiceBlocking;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.expensesMileageClaimReportingServiceFuture = expensesMileageClaimReportingServiceFuture;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.maintenanceWorkOrderCalculationServiceBlocking = maintenanceWorkOrderCalculationServiceBlocking;
        this.repository = repository;
        this.timekeepingTimesheetServiceBlocking = timekeepingTimesheetServiceBlocking;
    }

    public record EscalateExpenseReportStage0Requested(FindLedgerAccountsRequest request, CompletableFuture<FindLedgerAccountsResponse> completion, io.grpc.Context context) {}

    public record RecallExpenseReportStage1Requested(ExplainMediaAssetCalculationRequest request, CompletableFuture<ExplainMediaAssetCalculationResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an expense report.
     */
    public SubmitExpenseReportResponse submitExpenseReport(SubmitExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = QuoteChatRoomRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.chat.v1.ChatRoomCalculationService");
        var selected0 = ChatRoomCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quoteChatRoom(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitExpenseReport", request.getExpenseReport().getExpenseReportId());
        return SubmitExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getChatRoom().getAudit().getCreatedAt())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setQuantity(reply0.getChatRoom().getComponentsOrDefault("primary", ChatRoomComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .putAllMetadata(request.getExpenseReport().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Approves an expense report.
     */
    public ApproveExpenseReportResponse approveExpenseReport(ApproveExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = FindProductsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindProductsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getAudit().getUpdatedAt())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = catalogProductQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findProducts(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var results1 = new ArrayList<EstimateHouseholdResponse>();
        for (var element : request.getExpenseReport().getLinesList()) {
            EstimateHouseholdRequest itemRequest = EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .addChildren(Household.newBuilder()
                                                .putAllAttributes(element.getAttributesMap())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateHousehold(itemRequest));
        }
        var reply1 = results1.isEmpty() ? EstimateHouseholdResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveExpenseReport", request.getExpenseReport().getExpenseReportId());
        return ApproveExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(request.getExpenseReport().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects an expense report.
     */
    public RejectExpenseReportResponse rejectExpenseReport(RejectExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = GenerateMileageClaimReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = expensesMileageClaimReportingServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).generateMileageClaimReport(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectExpenseReport", request.getExpenseReport().getExpenseReportId());
        return RejectExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(request.getExpenseReport().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setOutcome(RejectExpenseReportResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Escalates an expense report.
     */
    public EscalateExpenseReportResponse escalateExpenseReport(EscalateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = FindLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getAudit().getCreatedAt())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<FindLedgerAccountsResponse>();
        events.publishEvent(new EscalateExpenseReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return EscalateExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(request.getExpenseReport().getBillingAddress())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setOutcome(EscalateExpenseReportResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists expense reports a page at a time.
     */
    public ListPendingExpenseReportsResponse listPendingExpenseReports(ListPendingExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "ListPendingExpenseReports");

        var outbound0 = CreateTimesheetRequest.newBuilder()
                .setContext(request.getContext())
                .setTimesheet(Timesheet.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(EmployeeRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setIssuedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var adapter0 = new Function<CreateTimesheetRequest, CreateTimesheetResponse>() {
            @Override
            public CreateTimesheetResponse apply(CreateTimesheetRequest input) {
                return timekeepingTimesheetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createTimesheet(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ScheduleExpensePolicyExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getTimesheet().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getTimesheet().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getTimesheet().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpensePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleExpensePolicyExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingExpenseReports", request.getPage().getPageToken());
        return ListPendingExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getTimesheet().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates an expense report.
     */
    public DelegateExpenseReportApprovalResponse delegateExpenseReportApproval(DelegateExpenseReportApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateExpenseReportApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        List<CalculateCustomerResponse> results0 = request.getExpenseReport().getLinesList().stream()
                .map(element -> CalculateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomer(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CalculateCustomerResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RejectWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectWriteOffRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getCustomer().getPrimaryAddress().getLinesList())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.WriteOffApprovalService");
        var selected1 = WriteOffApprovalServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.rejectWriteOff(outbound1);

        var outbound2 = CalculateChatRoomRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getCustomer().getPrimaryAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(chatChatRoomCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateChatRoom(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateExpenseReportApproval", request.getExpenseReport().getExpenseReportId());
        return DelegateExpenseReportApprovalResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getChatRoom().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getWriteOff().getBillingAddress().getRegion())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(request.getExpenseReport().getIssuedAt())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Recalls an expense report.
     */
    public RecallExpenseReportResponse recallExpenseReport(RecallExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CountLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getAudit().getCreatedAt())
                        .putAllLabels(request.getExpenseReport().getMetadataMap())
                        .setOwnerId(request.getExpenseReport().getCounterparty().getCustomerId())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter0 = new Function<CountLedgerAccountsRequest, CountLedgerAccountsResponse>() {
            @Override
            public CountLedgerAccountsResponse apply(CountLedgerAccountsRequest input) {
                return ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countLedgerAccounts(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ExplainMediaAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setMediaAsset(MediaAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getExpenseReport().getBillingAddress().getCountryCode())
                                .setLocality(request.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ExplainMediaAssetCalculationResponse>();
        events.publishEvent(new RecallExpenseReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = (QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getExpenseReport().getBillingAddress().getLinesList())
                                .setRecipient(request.getExpenseReport().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound2);

        var outbound3 = JsonCodec.roundTrip(ScoreExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build(), ScoreExpensePolicyRequest.class);
        var reply3 = expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExpensePolicy(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallExpenseReport", request.getExpenseReport().getExpenseReportId());
        return RecallExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getMediaAsset().getAddress().getRecipient())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(request.getExpenseReport().getIssuedAt())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply3.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Comments an expense report.
     */
    public CommentExpenseReportApprovalResponse commentExpenseReportApproval(CommentExpenseReportApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentExpenseReportApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ReconcileExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileExpensePoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setBlock(ExpensePolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ReconcileExpensePoliciesRequest, ReconcileExpensePoliciesResponse> port0 = RpcPort.adapt(expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::reconcileExpensePolicies);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CompareWorkOrderOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareWorkOrderOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getExpenseReport().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getExpensePolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = maintenanceWorkOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareWorkOrderOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentExpenseReportApproval", request.getExpenseReport().getExpenseReportId());
        return CommentExpenseReportApprovalResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setDueAt(request.getExpenseReport().getDueAt())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setOutcome(CommentExpenseReportApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
