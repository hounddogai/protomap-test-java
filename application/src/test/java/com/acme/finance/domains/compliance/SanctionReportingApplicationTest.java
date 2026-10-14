package com.acme.finance.domains.compliance;

import com.acme.cart.v1.Cart;
import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.cart.v1.CompareCartOptionsRequest;
import com.acme.cart.v1.CompareCartOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.DeleteSanctionReportRequest;
import com.acme.compliance.v1.DeleteSanctionReportResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GenerateSanctionReportRequest;
import com.acme.compliance.v1.GenerateSanctionReportResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.search.v1.CheckSearchIndexEligibilityRequest;
import com.acme.search.v1.CheckSearchIndexEligibilityResponse;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.subscriptions.v1.CreateSubscriptionRequest;
import com.acme.subscriptions.v1.CreateSubscriptionResponse;
import com.acme.subscriptions.v1.Subscription;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
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

class SanctionReportingApplicationTest {
    @Test
    void streamSanctionMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new SanctionReportingApplicationEvents(SearchIndexValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof SanctionReportingApplication.FetchSanctionReportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new SanctionReportingApplication(
                CartCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                events,
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                new SanctionReportingRequestMapperImpl(),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SubscriptionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamSanctionMetricsRequest.getDefaultInstance());
        var response = application.streamSanctionMetrics(request);
        var sent0 = channel.request("acme.subscriptions.v1.SubscriptionService/CreateSubscription");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "subscription", "holder", "email"));
        var sent1 = channel.request("acme.risk.v1.RiskScenarioApprovalService/RejectRiskScenario");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.subscriptions.v1.SubscriptionService/CreateSubscription"), "subscription", "audit", "created_at"),
                ProbeSupport.value(sent1, "risk_scenario", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.subscriptions.v1.SubscriptionService/CreateSubscription"), "subscription", "audit", "etag"),
                ProbeSupport.value(response, "sanction", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioApprovalService/RejectRiskScenario"), "risk_scenario", "audit", "created_at"),
                ProbeSupport.value(response, "sanction", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "sanction", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "sanction", "manager", "email"));
        var endpoint = new SanctionReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamSanctionMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamSanctionMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamSanctionMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new SanctionReportingApplicationEvents(SearchIndexValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckSearchIndexEligibilityResponse>();
        listener.on(new SanctionReportingApplication.FetchSanctionReportStage0Requested(ProbeSupport.sample(CheckSearchIndexEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
