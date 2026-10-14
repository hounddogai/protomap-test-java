package com.acme.finance.domains.contracts;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.banking.v1.AdvanceBankAccountWorkflowRequest;
import com.acme.banking.v1.AdvanceBankAccountWorkflowResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.checkout.v1.BatchGetCheckoutSessionsRequest;
import com.acme.checkout.v1.BatchGetCheckoutSessionsResponse;
import com.acme.checkout.v1.CheckoutSessionServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowResponse;
import com.acme.contracts.v1.BrowseContractClausesRequest;
import com.acme.contracts.v1.BrowseContractClausesResponse;
import com.acme.contracts.v1.CancelContractClauseWorkflowRequest;
import com.acme.contracts.v1.CancelContractClauseWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractLine;
import com.acme.contracts.v1.ContractLineTax;
import com.acme.contracts.v1.DelegateRenewalApprovalRequest;
import com.acme.contracts.v1.DelegateRenewalApprovalResponse;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.ExportContractClausesRequest;
import com.acme.contracts.v1.ExportContractClausesResponse;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.contracts.v1.InspectContractClauseWorkflowRequest;
import com.acme.contracts.v1.InspectContractClauseWorkflowResponse;
import com.acme.contracts.v1.ListContractClauseWorkflowsRequest;
import com.acme.contracts.v1.ListContractClauseWorkflowsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PauseContractClauseWorkflowRequest;
import com.acme.contracts.v1.PauseContractClauseWorkflowResponse;
import com.acme.contracts.v1.RecallContractRequest;
import com.acme.contracts.v1.RecallContractResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalApprovalServiceGrpc;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.ResumeContractClauseWorkflowRequest;
import com.acme.contracts.v1.ResumeContractClauseWorkflowResponse;
import com.acme.contracts.v1.RetryContractClauseWorkflowRequest;
import com.acme.contracts.v1.RetryContractClauseWorkflowResponse;
import com.acme.contracts.v1.SignalContractClauseWorkflowRequest;
import com.acme.contracts.v1.SignalContractClauseWorkflowResponse;
import com.acme.contracts.v1.StartContractClauseWorkflowRequest;
import com.acme.contracts.v1.StartContractClauseWorkflowResponse;
import com.acme.customers.v1.CloneCustomerRequest;
import com.acme.customers.v1.CloneCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagCondition;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.pricing.v1.ClonePriceListRequest;
import com.acme.pricing.v1.ClonePriceListResponse;
import com.acme.pricing.v1.PriceList;
import com.acme.pricing.v1.PriceListServiceGrpc;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCalculationServiceGrpc;
import com.acme.returns.v1.ReturnRequestLine;
import com.acme.returns.v1.SimulateReturnRequestRequest;
import com.acme.returns.v1.SimulateReturnRequestResponse;
import com.acme.risk.v1.ListRiskScenarioWorkflowsRequest;
import com.acme.risk.v1.ListRiskScenarioWorkflowsResponse;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.routing.v1.FindRoutesRequest;
import com.acme.routing.v1.FindRoutesResponse;
import com.acme.routing.v1.RouteQueryServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a contract clause from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ContractClauseWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractClauseWorkflowApplication.class);

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final CheckoutSessionServiceGrpc.CheckoutSessionServiceBlockingStub checkoutCheckoutSessionServiceBlocking;

    private final ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking;

    private final ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking;

    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalApprovalServiceGrpc.RenewalApprovalServiceBlockingStub contractsRenewalApprovalServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final ContractClauseWorkflowRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final PriceListServiceGrpc.PriceListServiceBlockingStub pricingPriceListServiceBlocking;

    private final ContractsRepository repository;

    private final ReturnRequestCalculationServiceGrpc.ReturnRequestCalculationServiceBlockingStub returnsReturnRequestCalculationServiceBlocking;

    private final RouteQueryServiceGrpc.RouteQueryServiceBlockingStub routingRouteQueryServiceBlocking;

    private final Executor rpcExecutor;

    public ContractClauseWorkflowApplication(
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            ChannelRegistry channels,
            CheckoutSessionServiceGrpc.CheckoutSessionServiceBlockingStub checkoutCheckoutSessionServiceBlocking,
            ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking,
            ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking,
            ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalApprovalServiceGrpc.RenewalApprovalServiceBlockingStub contractsRenewalApprovalServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            ContractClauseWorkflowRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            PriceListServiceGrpc.PriceListServiceBlockingStub pricingPriceListServiceBlocking,
            ContractsRepository repository,
            ReturnRequestCalculationServiceGrpc.ReturnRequestCalculationServiceBlockingStub returnsReturnRequestCalculationServiceBlocking,
            RouteQueryServiceGrpc.RouteQueryServiceBlockingStub routingRouteQueryServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingChartOfAccountsServiceAsync = accountingChartOfAccountsServiceAsync;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.channels = channels;
        this.checkoutCheckoutSessionServiceBlocking = checkoutCheckoutSessionServiceBlocking;
        this.contractsContractApprovalServiceBlocking = contractsContractApprovalServiceBlocking;
        this.contractsContractClauseApprovalServiceBlocking = contractsContractClauseApprovalServiceBlocking;
        this.contractsContractClauseQueryServiceBlocking = contractsContractClauseQueryServiceBlocking;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalApprovalServiceBlocking = contractsRenewalApprovalServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.pricingPriceListServiceBlocking = pricingPriceListServiceBlocking;
        this.repository = repository;
        this.returnsReturnRequestCalculationServiceBlocking = returnsReturnRequestCalculationServiceBlocking;
        this.routingRouteQueryServiceBlocking = routingRouteQueryServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    public record InspectContractClauseWorkflowStage1Requested(FindContractClausesRequest request, CompletableFuture<FindContractClausesResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a contract clause.
     */
    public StartContractClauseWorkflowResponse startContractClauseWorkflow(StartContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        List<CloneFeatureFlagResponse> results0 = request.getContractClause().getConditionsList().stream()
                .map(element -> CloneFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .addConditions(FeatureFlagCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneFeatureFlag(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CloneFeatureFlagResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = UpdateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFeatureFlag().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getFeatureFlag().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getFeatureFlag().getOwner().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCustomer(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = ExportContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getFeatureFlag().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getFeatureFlag().getParametersMap())
                        .setOwnerId(reply1.getCustomer().getRelationshipManager().getUserId())
                        .setRegion(reply1.getCustomer().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var stream2 = contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportContractClauses(outbound2);
        var reply2 = ExportContractClausesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = mapper.startContractClauseWorkflowStage3(request, reply0);
        var reply3 = contractsContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallContract(outbound3);

        var outbound4 = BatchCreateChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateChartOfAccountsesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateChartOfAccountsesResponse.getDefaultInstance());
        var input4 = accountingChartOfAccountsServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateChartOfAccountses(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getContractClause().getOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContractClause().getAudit().getCreatedAt())
                                .setCreatedBy(request.getContractClause().getAudit().getCreatedBy())
                                .setEtag(request.getContractClause().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input5 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return StartContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setOwner(request.getContractClause().getOwner())
                        .build())
                .setOutcome(StartContractClauseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a contract clause.
     */
    public AdvanceContractClauseWorkflowResponse advanceContractClauseWorkflow(AdvanceContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = CloneCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<CloneCustomerRequest, CloneCustomerResponse> port0 = RpcPort.adapt(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneCustomer);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ListRiskScenarioWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRiskScenarioWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCustomer().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getContractClause().getOwner().getUserId())
                        .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.RiskScenarioWorkflowService");
        var selected1 = RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.listRiskScenarioWorkflows(outbound1);

        var outbound2 = DelegateRenewalApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            DelegateRenewalApprovalResponse fetch(DelegateRenewalApprovalRequest input) {
                return contractsRenewalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateRenewalApproval(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return AdvanceContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setEffective(request.getContractClause().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .setEmail(reply0.getCustomer().getContact().getEmail())
                                .setUserId(reply2.getRenewal().getOrganizer().getUserId())
                                .build())
                        .build())
                .setOutcome(AdvanceContractClauseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a contract clause.
     */
    public CancelContractClauseWorkflowResponse cancelContractClauseWorkflow(CancelContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = JsonCodec.roundTrip(CountAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContractClause().getAudit().getCreatedAt())
                        .build())
                .build(), CountAuditEventsRequest.class);
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAuditEvents(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return CancelContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a contract clause.
     */
    public RetryContractClauseWorkflowResponse retryContractClauseWorkflow(RetryContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = FindRoutesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindRoutesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContractClause().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> routingRouteQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findRoutes(outbound0), rpcExecutor);

        var outbound1 = AdvanceBankAccountWorkflowRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .setHolder(CustomerRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AdvanceBankAccountWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceBankAccountWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = mapper.retryContractClauseWorkflowStage2(request, reply1);
        var reply2 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyLedgerAccountWatchers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return RetryContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getBankAccount().getAudit().getCreatedAt())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setDescription(request.getContractClause().getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getBankAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setOutcome(RetryContractClauseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a contract clause.
     */
    public PauseContractClauseWorkflowResponse pauseContractClauseWorkflow(PauseContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = BeanCopies.copy(ExplainObligationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContractClause().getOwner().getUserId())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build(), ExplainObligationDecisionRequest.class);
        var reply0 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainObligationDecision(outbound0);

        var outbound1 = JsonCodec.roundTrip(BrowseContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getObligation().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getObligation().getAssignee().getUserId())
                        .build())
                .build(), BrowseContractClausesRequest.class);
        var reply1 = contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseContractClauses(outbound1);

        var outbound2 = EstimateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getObligation().getRequester().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(reply0.getObligation().getCustomer().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateObligation(value));
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return PauseContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setVersion(request.getContractClause().getVersion())
                        .build())
                .setOutcome(PauseContractClauseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a contract clause.
     */
    public ResumeContractClauseWorkflowResponse resumeContractClauseWorkflow(ResumeContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = mapper.resumeContractClauseWorkflowStage0(request);
        var reply0 = contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateContractClause(outbound0);

        var outbound1 = mapper.resumeContractClauseWorkflowStage1(request, reply0);
        var reply1 = returnsReturnRequestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateReturnRequest(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return ResumeContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getReturnRequest().getAudit().getCreatedAt())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setEffective(reply0.getContractClause().getEffective())
                        .putAllParameters(request.getContractClause().getParametersMap())
                        .setVersion(reply0.getContractClause().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a contract clause.
     */
    public InspectContractClauseWorkflowResponse inspectContractClauseWorkflow(InspectContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContractClauseId(), "InspectContractClauseWorkflow");

        var outbound0 = BeanCopies.copy(BatchGetCheckoutSessionsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetCheckoutSessionsRequest.class);
        var reply0 = checkoutCheckoutSessionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetCheckoutSessions(outbound0);

        var outbound1 = FindContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var completion1 = new CompletableFuture<FindContractClausesResponse>();
        events.publishEvent(new InspectContractClauseWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        return InspectContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists contract clauses a page at a time.
     */
    public ListContractClauseWorkflowsResponse listContractClauseWorkflows(ListContractClauseWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListContractClauseWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "ListContractClauseWorkflows");

        var outbound0 = RpcSupport.unpack(Any.pack(ClonePriceListRequest.newBuilder()
                .setContext(request.getContext())
                .setPriceList(PriceList.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), ClonePriceListRequest.class);
        var reply0 = pricingPriceListServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).clonePriceList(outbound0);

        var outbound1 = BeanCopies.copy(ListFiscalPeriodVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodVersionsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build(), ListFiscalPeriodVersionsRequest.class);
        var reply1 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFiscalPeriodVersions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListContractClauseWorkflows", request.getFilter().getQuery());
        return ListContractClauseWorkflowsResponse.newBuilder()
                .addContractClauses(ContractClause.newBuilder()
                        .setOwner(reply0.getPriceList().getOwner())
                        .setVersion(reply0.getPriceList().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Signals a contract clause.
     */
    public SignalContractClauseWorkflowResponse signalContractClauseWorkflow(SignalContractClauseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalContractClauseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = CheckUserEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckUserEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.identity.v1.UserValidationService");
        var selected0 = UserValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.checkUserEligibility(outbound0);

        var outbound1 = BrowseContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getUser().getPassport().getExpiresAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getUser().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.contracts.v1.ContractClauseQueryService");
        var serviceName1 = "acme.contracts.v1" + "." + "ContractClauseQueryService";
        var methodName1 = "Bro" + "wseContractClauses";
        var descriptor1 = MethodDescriptor.<BrowseContractClausesRequest, BrowseContractClausesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseContractClausesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseContractClausesResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getUser().getPrimaryAddress().getCountryCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateMetricSeries(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalContractClauseWorkflow", request.getContractClause().getContractClauseId());
        return SignalContractClauseWorkflowResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUser().getAudit().getEtag())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply2.getMetricSeries().getOwner().getUserId())
                                .build())
                        .setPriority(request.getContractClause().getPriority())
                        .build())
                .setOutcome(SignalContractClauseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
