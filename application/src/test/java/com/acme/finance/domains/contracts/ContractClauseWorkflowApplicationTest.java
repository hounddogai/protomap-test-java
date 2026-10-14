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
import com.acme.finance.testing.ProbeSupport;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ContractClauseWorkflowApplicationTest {
    @Test
    void startContractClauseWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ContractClauseWorkflowApplicationEvents(ContractClauseQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ContractClauseWorkflowApplication.InspectContractClauseWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ContractClauseWorkflowApplication(
                ChartOfAccountsServiceGrpc.newStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CheckoutSessionServiceGrpc.newBlockingStub(channel),
                ContractApprovalServiceGrpc.newBlockingStub(channel),
                ContractClauseApprovalServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalApprovalServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                new ContractClauseWorkflowRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                PriceListServiceGrpc.newBlockingStub(channel),
                repository,
                ReturnRequestCalculationServiceGrpc.newBlockingStub(channel),
                RouteQueryServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(StartContractClauseWorkflowRequest.getDefaultInstance());
        var response = application.startContractClauseWorkflow(request);
        var sent0 = channel.request("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "conditions", "attribute"),
                ProbeSupport.value(sent0, "feature_flag", "conditions", "attribute"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "conditions", "value"),
                ProbeSupport.value(sent0, "feature_flag", "conditions", "value"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "conditions", "negate"),
                ProbeSupport.value(sent0, "feature_flag", "conditions", "negate"));
        var sent1 = channel.request("acme.customers.v1.CustomerService/UpdateCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "owner", "email"),
                ProbeSupport.value(sent1, "customer", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "audit", "updated_at"),
                ProbeSupport.value(sent1, "customer", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "audit", "created_at"),
                ProbeSupport.value(sent1, "customer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "email"),
                ProbeSupport.value(sent1, "customer", "relationship_manager", "email"));
        var sent2 = channel.request("acme.contracts.v1.ContractClauseQueryService/ExportContractClauses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/UpdateCustomer"), "customer", "primary_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/UpdateCustomer"), "customer", "relationship_manager", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "parameters"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.contracts.v1.ContractApprovalService/RecallContract");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(sent3, "contract", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "display_name"),
                ProbeSupport.value(sent3, "contract", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag"), "feature_flag", "owner", "user_id"),
                ProbeSupport.value(sent3, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/UpdateCustomer"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/UpdateCustomer"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/RecallContract"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/RecallContract"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause_id"),
                ProbeSupport.value(response, "contract_clause", "contract_clause_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner"),
                ProbeSupport.value(response, "contract_clause", "owner"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ContractClauseWorkflowApplicationEvents(ContractClauseQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<FindContractClausesResponse>();
        listener.on(new ContractClauseWorkflowApplication.InspectContractClauseWorkflowStage1Requested(ProbeSupport.sample(FindContractClausesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
