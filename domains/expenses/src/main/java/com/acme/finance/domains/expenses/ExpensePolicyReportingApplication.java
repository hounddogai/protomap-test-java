package com.acme.finance.domains.expenses;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.DeleteExpensePolicyReportRequest;
import com.acme.expenses.v1.DeleteExpensePolicyReportResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.FetchExpensePolicyReportRequest;
import com.acme.expenses.v1.FetchExpensePolicyReportResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.ListExpensePolicyReportsRequest;
import com.acme.expenses.v1.ListExpensePolicyReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ScheduleExpensePolicyReportRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyReportResponse;
import com.acme.expenses.v1.StreamExpensePolicyMetricsRequest;
import com.acme.expenses.v1.StreamExpensePolicyMetricsResponse;
import com.acme.expenses.v1.ValidateMileageClaimRequest;
import com.acme.expenses.v1.ValidateMileageClaimResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fleet.v1.VehicleServiceGrpc;
import com.acme.fleet.v1.WatchVehiclesRequest;
import com.acme.fleet.v1.WatchVehiclesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Builds and schedules reports about expense policies. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ExpensePolicyReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicyReportingApplication.class);

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking;

    private final VehicleServiceGrpc.VehicleServiceBlockingStub fleetVehicleServiceBlocking;

    private final CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final ExpensePolicyReportingRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public ExpensePolicyReportingApplication(
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking,
            VehicleServiceGrpc.VehicleServiceBlockingStub fleetVehicleServiceBlocking,
            CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            ExpensePolicyReportingRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.expensesMileageClaimValidationServiceBlocking = expensesMileageClaimValidationServiceBlocking;
        this.fleetVehicleServiceBlocking = fleetVehicleServiceBlocking;
        this.insuranceCoverageLifecycleServiceBlocking = insuranceCoverageLifecycleServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Generates an expense policy.
     */
    public GenerateExpensePolicyReportResponse generateExpensePolicyReport(GenerateExpensePolicyReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateExpensePolicyReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "GenerateExpensePolicyReport");

        var outbound0 = ListExpensePolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListExpensePolicyConflictsRequest.Filter.newBuilder()
                        .setOwnerId(request.getExpensePolicyId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listExpensePolicyConflicts(outbound0), rpcExecutor);

        var outbound1 = UpdateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCustomer(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateExpensePolicyReport", request.getDetails().getFormat());
        return GenerateExpensePolicyReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns an expense policy.
     */
    public FetchExpensePolicyReportResponse fetchExpensePolicyReport(FetchExpensePolicyReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchExpensePolicyReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "FetchExpensePolicyReport");

        var outbound0 = ScheduleCoverageExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        Function<ScheduleCoverageExpiryRequest, ScheduleCoverageExpiryResponse> operation0 = insuranceCoverageLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleCoverageExpiry;
        var reply0 = operation0.apply(outbound0);
        return FetchExpensePolicyReportResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .setEtag(reply0.getCoverage().getAudit().getEtag())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }

    /**
     * Schedules an expense policy.
     */
    public ScheduleExpensePolicyReportResponse scheduleExpensePolicyReport(ScheduleExpensePolicyReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleExpensePolicyReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "ScheduleExpensePolicyReport");

        var outbound0 = RecalculateCustomerSegmentsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments", outbound0);
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleExpensePolicyReport", request.getExpensePolicyId());
        return ScheduleExpensePolicyReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams the changes of expense policies as they happen.
     */
    public StreamExpensePolicyMetricsResponse streamExpensePolicyMetrics(StreamExpensePolicyMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamExpensePolicyMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getContext().getTraceId(), "StreamExpensePolicyMetrics");

        var outbound0 = mapper.streamExpensePolicyMetricsStage0(request);
        var reply0 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(outbound0);

        var outbound1 = WatchVehiclesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchVehiclesRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                        .setName(reply0.getCustomer().getContact().getName())
                        .build())
                .build();
        var stream1 = fleetVehicleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchVehicles(outbound1);
        var reply1 = WatchVehiclesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamExpensePolicyMetrics", request.getFilter().getQuery());
        return StreamExpensePolicyMetricsResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists expense policies a page at a time.
     */
    public ListExpensePolicyReportsResponse listExpensePolicyReports(ListExpensePolicyReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListExpensePolicyReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getContext().getTraceId(), "ListExpensePolicyReports");

        var outbound0 = JsonCodec.roundTrip(PreviewExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewExpensePolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), PreviewExpensePolicyRequest.class);
        var reply0 = expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewExpensePolicy(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListExpensePolicyReports", request.getPage().getPageToken());
        return ListExpensePolicyReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Counts expense policies by status.
     */
    public CountExpensePolicyTotalsResponse countExpensePolicyTotals(CountExpensePolicyTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountExpensePolicyTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getContext().getTraceId(), "CountExpensePolicyTotals");

        var outbound0 = RpcSupport.unpack(Any.pack(ValidateMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMileageClaimRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), ValidateMileageClaimRequest.class);
        var reply0 = expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMileageClaim(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountExpensePolicyTotals", request.getPage().getPageToken());
        return CountExpensePolicyTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes an expense policy.
     */
    public DeleteExpensePolicyReportResponse deleteExpensePolicyReport(DeleteExpensePolicyReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteExpensePolicyReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "DeleteExpensePolicyReport");

        var outbound0 = CountNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getExpensePolicyId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countNotifications(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = EstimateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .putAllAttributes(reply0.getResult().getDetailsMap())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.expenses.v1.ReceiptCalculationService");
        var serviceName1 = "acme.expenses.v1" + "." + "ReceiptCalculationService";
        var methodName1 = "Est" + "imateReceipt";
        var descriptor1 = MethodDescriptor.<EstimateReceiptRequest, EstimateReceiptResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateReceiptRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateReceiptResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getReceipt().getAudit().getCreatedBy())
                                .build())
                        .setBank(JournalEntryBankSettlement.newBuilder()
                                .setBic(switch (reply1.getReceipt().getSettlementCase()) {
                                    case BANK -> reply1.getReceipt().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getReceipt().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintJournalEntries(outbound2);
        return DeleteExpensePolicyReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
