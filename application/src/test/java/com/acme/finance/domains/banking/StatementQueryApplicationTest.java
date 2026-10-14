package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BrowseStatementsRequest;
import com.acme.banking.v1.BrowseStatementsResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.CountStatementsRequest;
import com.acme.banking.v1.CountStatementsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.FindStatementsRequest;
import com.acme.banking.v1.FindStatementsResponse;
import com.acme.banking.v1.LintMandatesRequest;
import com.acme.banking.v1.LintMandatesResponse;
import com.acme.banking.v1.LookupStatementRequest;
import com.acme.banking.v1.LookupStatementResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PeekStatementRequest;
import com.acme.banking.v1.PeekStatementResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.SummarizeStatementsRequest;
import com.acme.banking.v1.SummarizeStatementsResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.forms.v1.SuggestFormsRequest;
import com.acme.forms.v1.SuggestFormsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StatementQueryApplicationTest {
    @Test
    void exportStatementsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new StatementQueryApplicationEvents(AuditEventServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StatementQueryApplication.ExportStatementsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StatementQueryApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                FormQueryServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportStatementsRequest.getDefaultInstance());
        var response = application.exportStatements(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventService/SearchAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.banking.v1.MandateValidationService/ValidateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "mandate", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.banking.v1.MandateValidationService/LintMandates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "billing_address", "region"),
                ProbeSupport.value(sent2, "mandate", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "mandate", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "card", "token"),
                ProbeSupport.value(sent2, "mandate", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "billing_address", "lines"),
                ProbeSupport.value(sent2, "mandate", "billing_address", "lines"));
        var sent3 = channel.request("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "counterparty", "email"),
                ProbeSupport.value(sent3, "standing_order", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/SearchAuditEvents"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "number"),
                ProbeSupport.value(response, "statement", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/LintMandates"), "mandate", "billing_address", "postal_code"),
                ProbeSupport.value(response, "statement", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/LintMandates"), "mandate", "audit", "etag"),
                ProbeSupport.value(response, "statement", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder"), "standing_order", "standing_order_id"),
                ProbeSupport.value(response, "statement", "counterparty", "standing_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "statement", "billing_address", "region"));
        var endpoint = new StatementQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportStatementsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportStatementsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportStatements(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StatementQueryApplicationEvents(AuditEventServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SearchAuditEventsResponse>();
        listener.on(new StatementQueryApplication.ExportStatementsStage0Requested(ProbeSupport.sample(SearchAuditEventsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
