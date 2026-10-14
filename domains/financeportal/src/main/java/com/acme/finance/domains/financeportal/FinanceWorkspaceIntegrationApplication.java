package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordMeasurement;
import com.acme.customers.v1.ConsentRecordSyncServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdReportingServiceGrpc;
import com.acme.customers.v1.ListHouseholdReportsRequest;
import com.acme.customers.v1.ListHouseholdReportsResponse;
import com.acme.customers.v1.ListPreferenceReportsRequest;
import com.acme.customers.v1.ListPreferenceReportsResponse;
import com.acme.customers.v1.ReconcileConsentRecordsRequest;
import com.acme.customers.v1.ReconcileConsentRecordsResponse;
import com.acme.datahub.v1.BatchGetWorkspacesRequest;
import com.acme.datahub.v1.BatchGetWorkspacesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCardSettlement;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryMileageClaimWorkflowRequest;
import com.acme.expenses.v1.RetryMileageClaimWorkflowResponse;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.expenses.v1.StartMileageClaimWorkflowRequest;
import com.acme.expenses.v1.StartMileageClaimWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxCardSettlement;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceComment;
import com.acme.financeportal.v1.FinanceWorkspaceSla;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesRequest;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionRequest;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.lending.v1.CommentLoanApplicationApprovalRequest;
import com.acme.lending.v1.CommentLoanApplicationApprovalResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionBlockAction;
import com.acme.payroll.v1.DeductionWorkflowServiceGrpc;
import com.acme.payroll.v1.PauseDeductionWorkflowRequest;
import com.acme.payroll.v1.PauseDeductionWorkflowResponse;
import com.acme.suppliers.v1.ListPendingSupplierRisksRequest;
import com.acme.suppliers.v1.ListPendingSupplierRisksResponse;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Connects external systems that send finance workspace events. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FinanceWorkspaceIntegrationApplication {
    private static final Logger log = LoggerFactory.getLogger(FinanceWorkspaceIntegrationApplication.class);

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordSyncServiceGrpc.ConsentRecordSyncServiceBlockingStub customersConsentRecordSyncServiceBlocking;

    private final HouseholdReportingServiceGrpc.HouseholdReportingServiceBlockingStub customersHouseholdReportingServiceBlocking;

    private final ApplicationEventPublisher events;

    private final MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking;

    private final ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final FinanceWorkspaceIntegrationRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final DeductionWorkflowServiceGrpc.DeductionWorkflowServiceBlockingStub payrollDeductionWorkflowServiceBlocking;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceBlockingStub suppliersSupplierRatingWorkflowServiceBlocking;

    private final SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync;

    public FinanceWorkspaceIntegrationApplication(
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordSyncServiceGrpc.ConsentRecordSyncServiceBlockingStub customersConsentRecordSyncServiceBlocking,
            HouseholdReportingServiceGrpc.HouseholdReportingServiceBlockingStub customersHouseholdReportingServiceBlocking,
            ApplicationEventPublisher events,
            MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking,
            ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            FinanceWorkspaceIntegrationRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            DeductionWorkflowServiceGrpc.DeductionWorkflowServiceBlockingStub payrollDeductionWorkflowServiceBlocking,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceBlockingStub suppliersSupplierRatingWorkflowServiceBlocking,
            SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync) {
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordSyncServiceBlocking = customersConsentRecordSyncServiceBlocking;
        this.customersHouseholdReportingServiceBlocking = customersHouseholdReportingServiceBlocking;
        this.events = events;
        this.expensesMileageClaimWorkflowServiceBlocking = expensesMileageClaimWorkflowServiceBlocking;
        this.financeportalApprovalInboxWorkflowServiceBlocking = financeportalApprovalInboxWorkflowServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.payrollDeductionWorkflowServiceBlocking = payrollDeductionWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingWorkflowServiceBlocking = suppliersSupplierRatingWorkflowServiceBlocking;
        this.suppliersSupplierRiskApprovalServiceBlocking = suppliersSupplierRiskApprovalServiceBlocking;
        this.treasuryCashPositionServiceAsync = treasuryCashPositionServiceAsync;
    }

    public record RelayFinanceWorkspaceMessagesStage0Requested(BatchGetWorkspacesRequest request, CompletableFuture<BatchGetWorkspacesResponse> completion, io.grpc.Context context) {}

    /**
     * Connects a finance workspace.
     */
    public ConnectFinanceWorkspaceSourceResponse connectFinanceWorkspaceSource(ConnectFinanceWorkspaceSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ConnectFinanceWorkspaceSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "ConnectFinanceWorkspaceSource");

        var outbound0 = SignalMileageClaimWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalMileageClaimWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.MileageClaimWorkflowService");
        var selected0 = MileageClaimWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.signalMileageClaimWorkflow(outbound0);

        var outbound1 = mapper.connectFinanceWorkspaceSourceStage1(request, reply0);
        var reply1 = expensesMileageClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startMileageClaimWorkflow(outbound1);

        var outbound2 = JsonCodec.roundTrip(StartSupplierRatingWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getMileageClaim().getBillingAddress().getCountryCode())
                                .addAllLines(reply1.getMileageClaim().getBillingAddress().getLinesList())
                                .setRegion(reply1.getMileageClaim().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), StartSupplierRatingWorkflowRequest.class);
        var reply2 = suppliersSupplierRatingWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startSupplierRatingWorkflow(outbound2);

        var outbound3 = BatchCreateCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCashPositionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateCashPositionsResponse.getDefaultInstance());
        var input3 = treasuryCashPositionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCashPositions(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input4 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ConnectFinanceWorkspaceSource", request.getDetails().getEndpoint());
        return ConnectFinanceWorkspaceSourceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getMileageClaim().getAudit().getCreatedAt())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setOutcome(ConnectFinanceWorkspaceSourceResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Disconnects a finance workspace.
     */
    public DisconnectFinanceWorkspaceSourceResponse disconnectFinanceWorkspaceSource(DisconnectFinanceWorkspaceSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DisconnectFinanceWorkspaceSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "DisconnectFinanceWorkspaceSource");

        var outbound0 = ReconcileConsentRecordsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(ConsentRecordMeasurement.newBuilder()
                                .setObservedAt(request.getDetails().getReceivedAt())
                                .build())
                        .setSource(request.getDetails().getMessageId())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileConsentRecordsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersConsentRecordSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileConsentRecords(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CommentLoanApplicationApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLoanApplicationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addLines(LoanApplicationLine.newBuilder()
                                .putAllAttributes(reply0.getConsentRecord().getAttributesMap())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLoanApplicationApproval(value));

        var outbound2 = ListPreferenceReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPreferenceReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getLoanApplication().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply0.getOutcome().getReferenceId())
                        .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.PreferenceReportingService");
        var serviceName2 = "acme.customers.v1" + "." + "PreferenceReportingService";
        var methodName2 = "Lis" + "tPreferenceReports";
        var descriptor2 = MethodDescriptor.<ListPreferenceReportsRequest, ListPreferenceReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ListPreferenceReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListPreferenceReportsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ListHouseholdReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListHouseholdReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> customersHouseholdReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listHouseholdReports(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DisconnectFinanceWorkspaceSource", request.getDetails().getEndpoint());
        return DisconnectFinanceWorkspaceSourceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setOutcome(DisconnectFinanceWorkspaceSourceResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of finance workspaces and answers once at the end.
     */
    public IngestFinanceWorkspaceEventsResponse ingestFinanceWorkspaceEvents(IngestFinanceWorkspaceEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "IngestFinanceWorkspaceEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = SummarizeMetricSeriesesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SummarizeMetricSeriesesRequest.Details.newBuilder()
                        .putAllFilters(request.getRecord().getCustomFieldsMap())
                        .setFormat(request.getHeader().getFormat())
                        .build())
                .build();
        var adapter0 = new Function<SummarizeMetricSeriesesRequest, SummarizeMetricSeriesesResponse>() {
            @Override
            public SummarizeMetricSeriesesResponse apply(SummarizeMetricSeriesesRequest input) {
                return metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMetricSerieses(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var results1 = new ArrayList<ListApprovalInboxWorkflowsResponse>();
        for (var element : request.getRecord().getHistoryList()) {
            ListApprovalInboxWorkflowsRequest itemRequest = ListApprovalInboxWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListApprovalInboxWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getRecord().getCustomer().getSupplierId())
                        .build())
                .build();
            results1.add(financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listApprovalInboxWorkflows(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ListApprovalInboxWorkflowsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = RetryMileageClaimWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getCreatedBy();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = expensesMileageClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryMileageClaimWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "IngestFinanceWorkspaceEvents", request.getHeader().getSource());
        return IngestFinanceWorkspaceEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Replays a finance workspace.
     */
    public ReplayFinanceWorkspaceEventsResponse replayFinanceWorkspaceEvents(ReplayFinanceWorkspaceEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReplayFinanceWorkspaceEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "ReplayFinanceWorkspaceEvents");

        var outbound0 = SignalSupplierRatingWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierRatingWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalSupplierRatingWorkflow(value));

        var outbound1 = CancelApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getSupplierRating().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelApprovalInboxWorkflow(outbound1);

        var outbound2 = RevertJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertJournalEntryRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getSupplierRating().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .setCard(JournalEntryCardSettlement.newBuilder()
                                .setLast4(reply1.getApprovalInbox().getCard().getLast4())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            RevertJournalEntryResponse fetch(RevertJournalEntryRequest input) {
                return ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertJournalEntry(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReplayFinanceWorkspaceEvents", request.getDetails().getEndpoint());
        return ReplayFinanceWorkspaceEventsResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getApprovalInbox().getAudit().getUpdatedAt())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .setOutcome(ReplayFinanceWorkspaceEventsResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Acknowledges a finance workspace.
     */
    public AcknowledgeFinanceWorkspaceEventResponse acknowledgeFinanceWorkspaceEvent(AcknowledgeFinanceWorkspaceEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AcknowledgeFinanceWorkspaceEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "AcknowledgeFinanceWorkspaceEvent");

        var outbound0 = ResumeSupplierRatingWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ResumeSupplierRatingWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage0Adapter {
            ResumeSupplierRatingWorkflowResponse fetch(ResumeSupplierRatingWorkflowRequest input) {
                return suppliersSupplierRatingWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeSupplierRatingWorkflow(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ListPendingSupplierRisksRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingSupplierRisksRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSupplierRating().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> suppliersSupplierRiskApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingSupplierRisks(outbound1));

        var builder2 = SubscribeFiscalPeriodUpdatesRequest.newBuilder();
        builder2.putAllAnnotations(request.getAnnotationsMap());
        builder2.setContext(request.getContext());
        builder2.setDetails(SubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                        .build())
                .build());
        builder2.setReason(request.getReason());
        var outbound2 = builder2.build();
        var reply2 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeFiscalPeriodUpdates(outbound2);

        var outbound3 = (ResumeApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getFiscalPeriod().getLocation().getLinesList())
                                .build())
                        .setCard(ApprovalInboxCardSettlement.newBuilder()
                                .setLast4(reply0.getSupplierRating().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResumeApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeApprovalInboxWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AcknowledgeFinanceWorkspaceEvent", request.getDetails().getEndpoint());
        return AcknowledgeFinanceWorkspaceEventResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getApprovalInbox().getAudit().getEtag())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setOutcome(AcknowledgeFinanceWorkspaceEventResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply3.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of finance workspaces with a peer.
     */
    public RelayFinanceWorkspaceMessagesResponse relayFinanceWorkspaceMessages(RelayFinanceWorkspaceMessagesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RelayFinanceWorkspaceMessages")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = BatchGetWorkspacesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<BatchGetWorkspacesResponse>();
        events.publishEvent(new RelayFinanceWorkspaceMessagesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = StartApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getChange().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.financeportal.v1.ApprovalInboxWorkflowService/StartApprovalInboxWorkflow", outbound1);
        var reply1 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startApprovalInboxWorkflow(command1.payload());

        var futures2 = request.getChange().getCommentsList().stream()
                .map(element -> SignalApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SignalApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalApprovalInboxWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? SignalApprovalInboxWorkflowResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getChange().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RelayFinanceWorkspaceMessages", request.getChange().getFinanceWorkspaceId());
        return RelayFinanceWorkspaceMessagesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getApprovalInbox().getAudit().getCreatedAt())
                                .build())
                        .setRequester(request.getChange().getRequester())
                        .setStatus(request.getChange().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    public ListFinanceWorkspaceSourcesResponse listFinanceWorkspaceSources(ListFinanceWorkspaceSourcesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFinanceWorkspaceSources")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "ListFinanceWorkspaceSources");

        var outbound0 = RejectChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<RejectChartOfAccountsRequest, RejectChartOfAccountsResponse> port0 = RpcPort.adapt(accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::rejectChartOfAccounts);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFinanceWorkspaceSources", request.getPage().getPageToken());
        return ListFinanceWorkspaceSourcesResponse.newBuilder()
                .addFinanceWorkspaces(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Tests a finance workspace.
     */
    public TestFinanceWorkspaceConnectionResponse testFinanceWorkspaceConnection(TestFinanceWorkspaceConnectionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TestFinanceWorkspaceConnection")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "TestFinanceWorkspaceConnection");

        var outbound0 = PauseDeductionWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setBlock(DeductionBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        Function<PauseDeductionWorkflowRequest, PauseDeductionWorkflowResponse> operation0 = payrollDeductionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pauseDeductionWorkflow;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = RetrySupplierRatingWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getDeduction().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getDeduction().getOwner().getEmail())
                                .build())
                        .addLines(SupplierRatingLine.newBuilder()
                                .setTax(SupplierRatingLineTax.newBuilder()
                                        .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.suppliers.v1.SupplierRatingWorkflowService/RetrySupplierRatingWorkflow", outbound1);
        var reply1 = suppliersSupplierRatingWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retrySupplierRatingWorkflow(command1.payload());

        var outbound2 = JsonCodec.roundTrip(CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDeduction().getOwner().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(reply1.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CreateUserRequest.class);
        var reply2 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createUser(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TestFinanceWorkspaceConnection", request.getDetails().getEndpoint());
        return TestFinanceWorkspaceConnectionResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .setUpdatedAt(reply2.getUser().getAudit().getUpdatedAt())
                                .build())
                        .addComments(FinanceWorkspaceComment.newBuilder()
                                .setCreatedAt(reply2.getUser().getAudit().getCreatedAt())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getDeduction().getOwner().getEmail())
                                .build())
                        .setSla(FinanceWorkspaceSla.newBuilder()
                                .setDueAt(reply1.getSupplierRating().getDueAt())
                                .build())
                        .build())
                .build();
    }
}
