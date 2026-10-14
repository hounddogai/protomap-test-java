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
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ContractClauseWorkflowRequestMapperImpl implements ContractClauseWorkflowRequestMapper {
    @Override
    public RecallContractRequest startContractClauseWorkflowStage3(StartContractClauseWorkflowRequest request, CloneFeatureFlagResponse reply0) {
        return RecallContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .build())
                        .addLines(ContractLine.newBuilder()
                                .setTax(ContractLineTax.newBuilder()
                                        .setAmount(reply0.getFeatureFlag().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setDetails(RecallContractRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getFeatureFlag().getOwner().getUserId())
                                .build())
                        .build())
                .build();
    }

    @Override
    public NotifyLedgerAccountWatchersRequest retryContractClauseWorkflowStage2(RetryContractClauseWorkflowRequest request, AdvanceBankAccountWorkflowResponse reply1) {
        return NotifyLedgerAccountWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyLedgerAccountWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .setName(reply1.getBankAccount().getContact().getName())
                                .setPhone(reply1.getBankAccount().getContact().getPhone())
                                .build())
                        .build())
                .build();
    }

    @Override
    public EscalateContractClauseRequest resumeContractClauseWorkflowStage0(ResumeContractClauseWorkflowRequest request) {
        return EscalateContractClauseRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContractClause().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    @Override
    public SimulateReturnRequestRequest resumeContractClauseWorkflowStage1(ResumeContractClauseWorkflowRequest request, EscalateContractClauseResponse reply0) {
        return SimulateReturnRequestRequest.newBuilder()
                .setContext(request.getContext())
                .setReturnRequest(ReturnRequest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getContractClause().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .addLines(ReturnRequestLine.newBuilder()
                                .setAmount(request.getContractClause().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
    }
}
