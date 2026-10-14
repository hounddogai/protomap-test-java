package com.acme.finance.domains.contracts;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.broadcasts.v1.BroadcastQueryServiceGrpc;
import com.acme.broadcasts.v1.CountBroadcastsRequest;
import com.acme.broadcasts.v1.CountBroadcastsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.certificates.v1.CsrRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BrowseContractsRequest;
import com.acme.contracts.v1.BrowseContractsResponse;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.CountContractsRequest;
import com.acme.contracts.v1.CountContractsResponse;
import com.acme.contracts.v1.ExportContractsRequest;
import com.acme.contracts.v1.ExportContractsResponse;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.contracts.v1.LookupContractRequest;
import com.acme.contracts.v1.LookupContractResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PeekContractRequest;
import com.acme.contracts.v1.PeekContractResponse;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.SuggestContractsRequest;
import com.acme.contracts.v1.SuggestContractsResponse;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.labeling.v1.SimulateLabelingTaskRequest;
import com.acme.labeling.v1.SimulateLabelingTaskResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ContractQueryApplicationTest {
    @Test
    void exportContractsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var application = new ContractQueryApplication(
                AuditTrailNotificationServiceGrpc.newFutureStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                BroadcastQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                CertificateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newFutureStub(channel),
                LabelingTaskCalculationServiceGrpc.newBlockingStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new ContractQueryRequestMapperImpl(),
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportContractsRequest.getDefaultInstance());
        var response = application.exportContracts(request);
        var sent0 = channel.request("acme.contracts.v1.ObligationValidationService/PreviewObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "obligation", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/PreviewObligation"), "obligation", "audit", "created_by"),
                ProbeSupport.value(response, "contract", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/PreviewObligation"), "obligation", "customer", "display_name"),
                ProbeSupport.value(response, "contract", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "contract", "billing_address", "region"));
        var endpoint = new ContractQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportContractsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportContractsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportContracts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeContractsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var application = new ContractQueryApplication(
                AuditTrailNotificationServiceGrpc.newFutureStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                BroadcastQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                CertificateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newFutureStub(channel),
                LabelingTaskCalculationServiceGrpc.newBlockingStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new ContractQueryRequestMapperImpl(),
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeContractsRequest.getDefaultInstance());
        var response = application.summarizeContracts(request);
        var sent0 = channel.request("acme.broadcasts.v1.BroadcastQueryService/CountBroadcasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "format"),
                ProbeSupport.value(sent1, "format"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.broadcasts.v1.BroadcastQueryService/CountBroadcasts"), "result", "correlation_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "filters"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetValidationService/PreviewBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent2, "budget", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.broadcasts.v1.BroadcastQueryService/CountBroadcasts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/PreviewBudget"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
