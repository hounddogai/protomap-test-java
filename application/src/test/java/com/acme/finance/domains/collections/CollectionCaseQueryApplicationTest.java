package com.acme.finance.domains.collections;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CountCollectionCasesRequest;
import com.acme.collections.v1.CountCollectionCasesResponse;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.collections.v1.FindCollectionCasesRequest;
import com.acme.collections.v1.FindCollectionCasesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.LookupCollectionCaseRequest;
import com.acme.collections.v1.LookupCollectionCaseResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PeekCollectionCaseRequest;
import com.acme.collections.v1.PeekCollectionCaseResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ResolveCollectionCasesRequest;
import com.acme.collections.v1.ResolveCollectionCasesResponse;
import com.acme.collections.v1.SuggestCollectionCasesRequest;
import com.acme.collections.v1.SuggestCollectionCasesResponse;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.WatchPoliciesRequest;
import com.acme.insurance.v1.WatchPoliciesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
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
import org.springframework.stereotype.Service;

class CollectionCaseQueryApplicationTest {
    @Test
    void exportCollectionCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var application = new CollectionCaseQueryApplication(
                BankAccountServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportCollectionCasesRequest.getDefaultInstance());
        var response = application.exportCollectionCases(request);
        var sent0 = channel.request("acme.collections.v1.WriteOffValidationService/PreviewWriteOff");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "write_off", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "write_off", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        var sent1 = channel.request("acme.notifications.v1.NotificationValidationService/ValidateNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "audit", "created_at"),
                ProbeSupport.value(sent1, "notification", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "audit", "created_by"),
                ProbeSupport.value(sent1, "notification", "audit", "created_by"));
        var sent2 = channel.request("acme.banking.v1.MandateValidationService/ScoreMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "audit", "etag"),
                ProbeSupport.value(sent2, "mandate", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "mandate", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "audit", "updated_at"),
                ProbeSupport.value(sent2, "mandate", "audit", "updated_at"));
        var sent3 = channel.request("acme.collections.v1.WriteOffValidationService/ValidateWriteOff");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "write_off", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "audit", "etag"),
                ProbeSupport.value(response, "collection_case", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "counterparty", "collection_case_id"),
                ProbeSupport.value(response, "collection_case", "collection_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "email"),
                ProbeSupport.value(response, "collection_case", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ScoreMandate"), "mandate", "audit", "created_at"),
                ProbeSupport.value(response, "collection_case", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "counterparty", "priority"),
                ProbeSupport.value(response, "collection_case", "priority"));
        var endpoint = new CollectionCaseQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportCollectionCasesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportCollectionCasesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportCollectionCases(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeCollectionCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var application = new CollectionCaseQueryApplication(
                BankAccountServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeCollectionCasesRequest.getDefaultInstance());
        var response = application.summarizeCollectionCases(request);
        var sent0 = channel.request("acme.collections.v1.WriteOffValidationService/LintWriteOffs");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case_id"),
                ProbeSupport.value(sent0, "write_off", "counterparty", "collection_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.insurance.v1.PolicyCalculationService/CalculatePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/LintWriteOffs"), "write_off", "audit", "etag"),
                ProbeSupport.value(sent1, "policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/LintWriteOffs"), "write_off", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        var sent2 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/CalculatePolicy"), "policy", "audit", "updated_at"),
                ProbeSupport.value(sent2, "feature_flag", "audit", "updated_at"));
        var sent3 = channel.request("acme.collections.v1.WriteOffValidationService/ExplainWriteOffDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(sent3, "write_off", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/CalculatePolicy"), "policy", "audit", "etag"),
                ProbeSupport.value(sent3, "write_off", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/CalculatePolicy"), "policy", "audit", "updated_by"),
                ProbeSupport.value(sent3, "write_off", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/LintWriteOffs"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/LintWriteOffs"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
