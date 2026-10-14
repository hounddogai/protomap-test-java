package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.BatchGetBankAccountsRequest;
import com.acme.banking.v1.BatchGetBankAccountsResponse;
import com.acme.banking.v1.CloneBankAccountRequest;
import com.acme.banking.v1.CloneBankAccountResponse;
import com.acme.banking.v1.CreateBankAccountRequest;
import com.acme.banking.v1.CreateBankAccountResponse;
import com.acme.banking.v1.DeleteBankAccountRequest;
import com.acme.banking.v1.DeleteBankAccountResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.GetBankAccountRequest;
import com.acme.banking.v1.GetBankAccountResponse;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateBankSettlement;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.SearchBankAccountsRequest;
import com.acme.banking.v1.SearchBankAccountsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.WatchBankAccountsRequest;
import com.acme.banking.v1.WatchBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.corporateevents.v1.SearchCorporateEventsRequest;
import com.acme.corporateevents.v1.SearchCorporateEventsResponse;
import com.acme.corporateevents.v1.WatchCorporateEventsRequest;
import com.acme.corporateevents.v1.WatchCorporateEventsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.SearchUsersRequest;
import com.acme.identity.v1.SearchUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.lineage.v1.CalculateLineageNodeRequest;
import com.acme.lineage.v1.CalculateLineageNodeResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class BankAccountApplicationTest {
    @Test
    void deleteBankAccountPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new BankAccountApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BankAccountApplication.GetBankAccountStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BankAccountApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newBlockingStub(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                new BankAccountRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(DeleteBankAccountRequest.getDefaultInstance());
        var response = application.deleteBankAccount(request);
        var sent0 = channel.request("acme.corporateevents.v1.CorporateEventService/SearchCorporateEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_account_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "baggage"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "etag"),
                ProbeSupport.value(sent1, "premium", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "billing_address", "lines"),
                ProbeSupport.value(sent2, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "billing_address", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "details", "destination", "postal_code"));
        var sent3 = channel.request("acme.banking.v1.MandateValidationService/NormalizeMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "audit", "updated_at"),
                ProbeSupport.value(sent3, "mandate", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.corporateevents.v1.CorporateEventService/SearchCorporateEvents"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.corporateevents.v1.CorporateEventService/SearchCorporateEvents"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/NormalizeMandate"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
    }

    @Test
    void watchBankAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new BankAccountApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BankAccountApplication.GetBankAccountStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BankAccountApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newBlockingStub(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                new BankAccountRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(WatchBankAccountsRequest.getDefaultInstance());
        var response = application.watchBankAccounts(request);
        var sent0 = channel.request("acme.lineage.v1.LineageNodeCalculationService/CalculateLineageNode");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "lineage_node", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "lineage_node", "children", "children", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/CalculateLineageNode"), "lineage_node", "audit", "updated_by"),
                ProbeSupport.value(response, "bank_account", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/CalculateLineageNode"), "lineage_node", "audit", "created_at"),
                ProbeSupport.value(response, "bank_account", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "bank_account", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(response, "bank_account", "contact", "preferred_channel"));
        var endpoint = new BankAccountEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchBankAccountsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchBankAccountsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchBankAccounts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateBankAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new BankAccountApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BankAccountApplication.GetBankAccountStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BankAccountApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newBlockingStub(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                new BankAccountRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(BatchCreateBankAccountsRequest.getDefaultInstance());
        var response = application.batchCreateBankAccounts(request);
        var sent0 = channel.request("acme.banking.v1.MandateValidationService/ValidateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "limits", "amount"),
                ProbeSupport.value(sent0, "mandate", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "etag"),
                ProbeSupport.value(sent0, "mandate", "audit", "etag"));
        var sent1 = channel.request("acme.corporateevents.v1.CorporateEventService/WatchCorporateEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "contact", "phone"),
                ProbeSupport.value(sent1, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var endpoint = new BankAccountEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateBankAccountsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateBankAccountsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateBankAccounts(observer);
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
        var listener = new BankAccountApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainMandateDecisionResponse>();
        listener.on(new BankAccountApplication.GetBankAccountStage1Requested(ProbeSupport.sample(ExplainMandateDecisionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
