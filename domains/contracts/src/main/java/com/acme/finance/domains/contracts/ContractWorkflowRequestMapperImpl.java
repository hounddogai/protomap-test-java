package com.acme.finance.domains.contracts;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.AdvanceContractWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractWorkflowResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractLine;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.InspectContractWorkflowRequest;
import com.acme.contracts.v1.InspectContractWorkflowResponse;
import com.acme.contracts.v1.ListContractWorkflowsRequest;
import com.acme.contracts.v1.ListContractWorkflowsResponse;
import com.acme.contracts.v1.ListPendingContractsRequest;
import com.acme.contracts.v1.ListPendingContractsResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.PauseContractWorkflowRequest;
import com.acme.contracts.v1.PauseContractWorkflowResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResumeContractWorkflowRequest;
import com.acme.contracts.v1.ResumeContractWorkflowResponse;
import com.acme.contracts.v1.RetryContractWorkflowRequest;
import com.acme.contracts.v1.RetryContractWorkflowResponse;
import com.acme.contracts.v1.SignalContractWorkflowRequest;
import com.acme.contracts.v1.SignalContractWorkflowResponse;
import com.acme.contracts.v1.StartContractWorkflowRequest;
import com.acme.contracts.v1.StartContractWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.EstimatePreferenceRequest;
import com.acme.customers.v1.EstimatePreferenceResponse;
import com.acme.customers.v1.ExplainPreferenceDecisionRequest;
import com.acme.customers.v1.ExplainPreferenceDecisionResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.SearchCustomersRequest;
import com.acme.customers.v1.SearchCustomersResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ContractWorkflowRequestMapperImpl implements ContractWorkflowRequestMapper {
    @Override
    public CreateContractRequest startContractWorkflowStage1(StartContractWorkflowRequest request) {
        return CreateContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getContract().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ListPendingContractsRequest retryContractWorkflowStage2(RetryContractWorkflowRequest request, ReconcileForecastsResponse reply0, CommentLoanApprovalResponse reply1) {
        return ListPendingContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getLoan().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getLoan().getLabelsMap())
                        .setOwnerId(reply0.getForecast().getCounterparty().getBudgetId())
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
