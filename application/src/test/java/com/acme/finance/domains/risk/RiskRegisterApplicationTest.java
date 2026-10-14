package com.acme.finance.domains.risk;

import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CloneRiskRegisterRequest;
import com.acme.risk.v1.CloneRiskRegisterResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.CreateRiskRegisterRequest;
import com.acme.risk.v1.CreateRiskRegisterResponse;
import com.acme.risk.v1.DeleteRiskRegisterRequest;
import com.acme.risk.v1.DeleteRiskRegisterResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetRiskRegisterRequest;
import com.acme.risk.v1.GetRiskRegisterResponse;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.SearchRiskRegistersRequest;
import com.acme.risk.v1.SearchRiskRegistersResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.SummarizeRiskScenariosRequest;
import com.acme.risk.v1.SummarizeRiskScenariosResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RiskRegisterApplicationTest {
    @Test
    void watchRiskRegistersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new RiskRegisterApplicationEvents(LoanApplicationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RiskRegisterApplication.ListRiskRegistersStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RiskRegisterApplication(
                ChannelRegistry.fixed(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                new RiskRegisterRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newFutureStub(channel),
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchRiskRegistersRequest.getDefaultInstance());
        var response = application.watchRiskRegisters(request);
        var sent0 = channel.request("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "loan_application", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "loan_application", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        var sent1 = channel.request("acme.lending.v1.CollateralValidationService/PreviewCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "collateral", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "billing_address", "locality"),
                ProbeSupport.value(sent1, "collateral", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "billing_address", "lines"),
                ProbeSupport.value(sent1, "collateral", "address", "lines"));
        var sent2 = channel.request("acme.insurance.v1.UnderwritingCalculationService/CompareUnderwritingOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "audit", "created_at"),
                ProbeSupport.value(sent2, "underwriting", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "audit", "etag"),
                ProbeSupport.value(sent2, "underwriting", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "address", "lines"),
                ProbeSupport.value(sent2, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "audit", "etag"),
                ProbeSupport.value(response, "risk_register", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/NormalizeLoanApplication"), "loan_application", "audit", "updated_at"),
                ProbeSupport.value(response, "risk_register", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "address", "lines"),
                ProbeSupport.value(response, "risk_register", "children", "children", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/CompareUnderwritingOptions"), "underwriting", "assignee", "email"),
                ProbeSupport.value(response, "risk_register", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "risk_register", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "risk_register", "location", "region"));
        var endpoint = new RiskRegisterEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchRiskRegistersResponse> observer = new StreamObserver<>() {
            public void onNext(WatchRiskRegistersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchRiskRegisters(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateRiskRegistersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new RiskRegisterApplicationEvents(LoanApplicationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RiskRegisterApplication.ListRiskRegistersStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RiskRegisterApplication(
                ChannelRegistry.fixed(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                new RiskRegisterRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newFutureStub(channel),
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateRiskRegistersRequest.getDefaultInstance());
        var response = application.batchCreateRiskRegisters(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingCalculationService/CompareUnderwritingOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent0, "underwriting", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_by"),
                ProbeSupport.value(sent0, "underwriting", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "manager", "email"),
                ProbeSupport.value(sent0, "underwriting", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "location", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterQueryService/ResolveRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/CompareUnderwritingOptions"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/CompareUnderwritingOptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/ResolveRiskRegisters"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        var endpoint = new RiskRegisterEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateRiskRegistersResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateRiskRegistersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateRiskRegisters(observer);
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
        var listener = new RiskRegisterApplicationEvents(LoanApplicationValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainLoanApplicationDecisionResponse>();
        listener.on(new RiskRegisterApplication.ListRiskRegistersStage0Requested(ProbeSupport.sample(ExplainLoanApplicationDecisionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
