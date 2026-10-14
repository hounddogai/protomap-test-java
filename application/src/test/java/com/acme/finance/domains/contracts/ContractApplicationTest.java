package com.acme.finance.domains.contracts;

import com.acme.collections.v1.ExportPaymentPromisesRequest;
import com.acme.collections.v1.ExportPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseStatusChange;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.BatchGetContractsRequest;
import com.acme.contracts.v1.BatchGetContractsResponse;
import com.acme.contracts.v1.CloneContractRequest;
import com.acme.contracts.v1.CloneContractResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractsRequest;
import com.acme.contracts.v1.CountContractsResponse;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.GetContractRequest;
import com.acme.contracts.v1.GetContractResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.ListContractsRequest;
import com.acme.contracts.v1.ListContractsResponse;
import com.acme.contracts.v1.LookupContractRequest;
import com.acme.contracts.v1.LookupContractResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.SearchContractsRequest;
import com.acme.contracts.v1.SearchContractsResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.contracts.v1.UpdateContractRequest;
import com.acme.contracts.v1.UpdateContractResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.contracts.v1.WatchContractsRequest;
import com.acme.contracts.v1.WatchContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ContractApplicationTest {
    @Test
    void updateContractPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ContractApplicationEvents(FeatureFlagServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ContractApplication.UpdateContractStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ContractApplication(
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                events,
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                new ContractRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(UpdateContractRequest.getDefaultInstance());
        var response = application.updateContract(request);
        var sent0 = channel.request("acme.contracts.v1.ContractQueryService/LookupContract");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "contract_id"),
                ProbeSupport.value(sent0, "contract_id"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "bank", "bic"),
                ProbeSupport.value(sent1, "ledger_account", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractQueryService/LookupContract"), "contract", "audit", "updated_at"),
                ProbeSupport.value(sent1, "ledger_account", "audit", "updated_at"));
        var sent2 = channel.request("acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.contracts.v1.ContractQueryService/CountContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractQueryService/LookupContract"), "contract", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractQueryService/LookupContract"), "contract", "audit", "updated_at"),
                ProbeSupport.value(response, "contract", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "issued_at"),
                ProbeSupport.value(response, "contract", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "contract_id"),
                ProbeSupport.value(response, "contract", "contract_id"));
    }

    @Test
    void watchContractsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ContractApplicationEvents(FeatureFlagServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ContractApplication.UpdateContractStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ContractApplication(
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                events,
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                new ContractRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(WatchContractsRequest.getDefaultInstance());
        var response = application.watchContracts(request);
        var sent0 = channel.request("acme.contracts.v1.ContractQueryService/SummarizeContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "baggage"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractQueryService/SummarizeContracts"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractQueryService/SummarizeContracts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "contract", "billing_address", "region"));
        var endpoint = new ContractEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchContractsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchContractsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchContracts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateContractsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ContractApplicationEvents(FeatureFlagServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ContractApplication.UpdateContractStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ContractApplication(
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                events,
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                new ContractRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(BatchCreateContractsRequest.getDefaultInstance());
        var response = application.batchCreateContracts(request);
        var sent0 = channel.request("acme.contracts.v1.ObligationCalculationService/SimulateObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "name"),
                ProbeSupport.value(sent0, "obligation", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/SimulateObligation"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/SimulateObligation"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        var endpoint = new ContractEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateContractsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateContractsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateContracts(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ContractApplicationEvents(FeatureFlagServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BatchGetFeatureFlagsResponse>();
        listener.on(new ContractApplication.UpdateContractStage2Requested(ProbeSupport.sample(BatchGetFeatureFlagsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
