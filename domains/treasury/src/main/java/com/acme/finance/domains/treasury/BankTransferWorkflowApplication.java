package com.acme.finance.domains.treasury;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.helpdesk.v1.FindTicketsRequest;
import com.acme.helpdesk.v1.FindTicketsResponse;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchAttachment;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.loyalty.v1.BrowseLoyaltyMembersRequest;
import com.acme.loyalty.v1.BrowseLoyaltyMembersResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowRequest;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CancelBankTransferWorkflowRequest;
import com.acme.treasury.v1.CancelBankTransferWorkflowResponse;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CountBankTransferTotalsRequest;
import com.acme.treasury.v1.CountBankTransferTotalsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.InspectBankTransferWorkflowRequest;
import com.acme.treasury.v1.InspectBankTransferWorkflowResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLine;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListBankTransferWorkflowsRequest;
import com.acme.treasury.v1.ListBankTransferWorkflowsResponse;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.PauseBankTransferWorkflowRequest;
import com.acme.treasury.v1.PauseBankTransferWorkflowResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.ResumeBankTransferWorkflowRequest;
import com.acme.treasury.v1.ResumeBankTransferWorkflowResponse;
import com.acme.treasury.v1.RetryBankTransferWorkflowRequest;
import com.acme.treasury.v1.RetryBankTransferWorkflowResponse;
import com.acme.treasury.v1.ScoreFxHedgeRequest;
import com.acme.treasury.v1.ScoreFxHedgeResponse;
import com.acme.treasury.v1.SignalBankTransferWorkflowRequest;
import com.acme.treasury.v1.SignalBankTransferWorkflowResponse;
import com.acme.treasury.v1.StartBankTransferWorkflowRequest;
import com.acme.treasury.v1.StartBankTransferWorkflowResponse;
import com.acme.treasury.v1.StreamBankTransferMetricsRequest;
import com.acme.treasury.v1.StreamBankTransferMetricsResponse;
import com.acme.treasury.v1.SubmitFxHedgeRequest;
import com.acme.treasury.v1.SubmitFxHedgeResponse;
import com.acme.treasury.v1.SuggestBankTransfersRequest;
import com.acme.treasury.v1.SuggestBankTransfersResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
 * Runs the long-lived workflow that takes a bank transfer from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BankTransferWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferWorkflowApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final TicketQueryServiceGrpc.TicketQueryServiceBlockingStub helpdeskTicketQueryServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final BankTransferWorkflowRequestMapper mapper;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking;

    private final BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking;

    private final FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    public BankTransferWorkflowApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            TicketQueryServiceGrpc.TicketQueryServiceBlockingStub helpdeskTicketQueryServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            BankTransferWorkflowRequestMapper mapper,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking,
            BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking,
            FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.helpdeskTicketQueryServiceBlocking = helpdeskTicketQueryServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.treasuryBankTransferQueryServiceBlocking = treasuryBankTransferQueryServiceBlocking;
        this.treasuryBankTransferReportingServiceBlocking = treasuryBankTransferReportingServiceBlocking;
        this.treasuryFxHedgeApprovalServiceBlocking = treasuryFxHedgeApprovalServiceBlocking;
        this.treasuryFxHedgeValidationServiceBlocking = treasuryFxHedgeValidationServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
    }

    /**
     * Starts a bank transfer.
     */
    public StartBankTransferWorkflowResponse startBankTransferWorkflow(StartBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = StreamBankTransferMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamBankTransferMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotify().getEmail())
                        .setName(request.getDetails().getNotify().getName())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build();
        var stream0 = treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamBankTransferMetrics(outbound0);
        var reply0 = StreamBankTransferMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getBankTransfer().getBankTransferId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input1 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input2 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartBankTransferWorkflow", request.getBankTransfer().getNumber());
        return StartBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setDueAt(request.getBankTransfer().getDueAt())
                        .build())
                .build();
    }

    /**
     * Advances a bank transfer.
     */
    public AdvanceBankTransferWorkflowResponse advanceBankTransferWorkflow(AdvanceBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = SubmitFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBankTransfer().getAudit().getCreatedBy())
                                .build())
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setHolderName(request.getBankTransfer().getCard().getHolderName())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryFxHedgeApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitFxHedge(outbound0), rpcExecutor);

        var outbound1 = CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateHousehold(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ListLiquidityForecastImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLiquidityForecastImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getHousehold().getAudit().getUpdatedAt())
                        .setOwnerId(reply1.getHousehold().getManager().getUserId())
                        .setRegion(reply0.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLiquidityForecastImports(outbound2));

        var outbound3 = LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(LintCustomersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.customers.v1.CustomerValidationService");
        var selected3 = CustomerValidationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.lintCustomers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return AdvanceBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply3.getCustomer().getPrimaryAddress().getCountryCode())
                                .addAllLines(reply1.getHousehold().getLocation().getLinesList())
                                .setRegion(reply1.getHousehold().getLocation().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply3.getCustomer().getCustomerId())
                                .setDisplayName(reply0.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .setNotes(request.getBankTransfer().getNotes())
                        .build())
                .setOutcome(AdvanceBankTransferWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Cancels a bank transfer.
     */
    public CancelBankTransferWorkflowResponse cancelBankTransferWorkflow(CancelBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ScorePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setBank(PostingBatchBankSettlement.newBuilder()
                                .setIban(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getBankTransfer().getBillingAddress().getRecipient())
                                .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePostingBatch(outbound0);

        var outbound1 = CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getBankTransfer().getAudit().getCreatedBy())
                                                .setUpdatedBy(reply0.getPostingBatch().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CheckHouseholdEligibilityRequest, CheckHouseholdEligibilityResponse>() {
            @Override
            public CheckHouseholdEligibilityResponse apply(CheckHouseholdEligibilityRequest input) {
                return customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return CancelBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(request.getBankTransfer().getBillingAddress())
                        .setIssuedAt(reply0.getPostingBatch().getIssuedAt())
                        .build())
                .setOutcome(CancelBankTransferWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a bank transfer.
     */
    public RetryBankTransferWorkflowResponse retryBankTransferWorkflow(RetryBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ScoreFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBank(FxHedgeBankSettlement.newBuilder()
                                .setIban(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getBankTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply0 = treasuryFxHedgeValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreFxHedge(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return RetryBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFxHedge().getAudit().getEtag())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setDueAt(request.getBankTransfer().getDueAt())
                        .build())
                .build();
    }

    /**
     * Pauses a bank transfer.
     */
    public PauseBankTransferWorkflowResponse pauseBankTransferWorkflow(PauseBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ValidateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(request.getBankTransfer().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(request.getBankTransfer().getBillingAddress().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateHousehold(outbound0), rpcExecutor);

        var outbound1 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BrowseLoyaltyMembersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoyaltyMembersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getHousehold().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getHousehold().getHouseholdId())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.loyalty.v1.LoyaltyMemberQueryService");
        var serviceName2 = "acme.loyalty.v1" + "." + "LoyaltyMemberQueryService";
        var methodName2 = "Bro" + "wseLoyaltyMembers";
        var descriptor2 = MethodDescriptor.<BrowseLoyaltyMembersRequest, BrowseLoyaltyMembersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseLoyaltyMembersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseLoyaltyMembersResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return PauseBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getHousehold().getAudit().getEtag())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setIssuedAt(request.getBankTransfer().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a bank transfer.
     */
    public ResumeBankTransferWorkflowResponse resumeBankTransferWorkflow(ResumeBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ListJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntryVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBankTransfer().getAudit().getUpdatedAt())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        Function<ListJournalEntryVersionsRequest, ListJournalEntryVersionsResponse> operation0 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listJournalEntryVersions;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getBankTransfer().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSearchIndex(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return ResumeBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(request.getBankTransfer().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getSearchIndex().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ResumeBankTransferWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a bank transfer.
     */
    public InspectBankTransferWorkflowResponse inspectBankTransferWorkflow(InspectBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "InspectBankTransferWorkflow");

        var outbound0 = PullLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullLiquidityForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(LiquidityForecastLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        Function<PullLiquidityForecastsRequest, PullLiquidityForecastsResponse> operation0 = treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pullLiquidityForecasts;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = QuoteCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setDetails(QuoteCustomerRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomer(outbound1);
        return InspectBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCustomer().getAudit().getEtag())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .putAllMetadata(reply0.getLiquidityForecast().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public ListBankTransferWorkflowsResponse listBankTransferWorkflows(ListBankTransferWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBankTransferWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "ListBankTransferWorkflows");

        var outbound0 = FindTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindTicketsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.helpdesk.v1.TicketQueryService/FindTickets", outbound0);
        var reply0 = helpdeskTicketQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findTickets(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBankTransferWorkflows", request.getPage().getPageToken());
        return ListBankTransferWorkflowsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }

    /**
     * Signals a bank transfer.
     */
    public SignalBankTransferWorkflowResponse signalBankTransferWorkflow(SignalBankTransferWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalBankTransferWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = mapper.signalBankTransferWorkflowStage0(request);
        var reply0 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLiquidityForecastImports(outbound0);

        var results1 = new ArrayList<CheckPostingBatchEligibilityResponse>();
        for (var element : request.getBankTransfer().getAttachmentsList()) {
            CheckPostingBatchEligibilityRequest itemRequest = CheckPostingBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addAttachments(PostingBatchAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build();
            results1.add(ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPostingBatchEligibility(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CheckPostingBatchEligibilityResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = SuggestBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBankTransfersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBankTransfer().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getPostingBatch().getMetadataMap())
                        .setOwnerId(reply1.getPostingBatch().getCounterparty().getLedgerAccountId())
                        .setRegion(reply1.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<SuggestBankTransfersRequest, SuggestBankTransfersResponse> port2 = RpcPort.adapt(treasuryBankTransferQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestBankTransfers);
        var reply2 = port2.exchange(outbound2);

        List<CountBankTransferTotalsResponse> results3 = request.getBankTransfer().getLinesList().stream()
                .map(element -> CountBankTransferTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBankTransferTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getPostingBatch().getAudit().getUpdatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply1.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBankTransferTotals(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? CountBankTransferTotalsResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalBankTransferWorkflow", request.getBankTransfer().getBankTransferId());
        return SignalBankTransferWorkflowResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setIssuedAt(request.getBankTransfer().getIssuedAt())
                        .build())
                .setOutcome(SignalBankTransferWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
