package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetStandingOrderRulesRequest;
import com.acme.banking.v1.GetStandingOrderRulesResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
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
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.ExportClaimsRequest;
import com.acme.insurance.v1.ExportClaimsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.shows.v1.CompareShowOptionsRequest;
import com.acme.shows.v1.CompareShowOptionsResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.shows.v1.ShowDailyRule;
import com.acme.shows.v1.ShowParticipant;
import com.acme.venues.v1.NormalizeVenueRequest;
import com.acme.venues.v1.NormalizeVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
import org.springframework.stereotype.Service;

class StandingOrderValidationApplicationTest {
    @Test
    void normalizeStandingOrderPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var application = new StandingOrderValidationApplication(
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserServiceGrpc.newBlockingStub(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                new StandingOrderValidationRequestMapperImpl(),
                repository,
                Runnable::run,
                ShowCalculationServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(NormalizeStandingOrderRequest.getDefaultInstance());
        var response = application.normalizeStandingOrder(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryValidationService/LintJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        var sent1 = channel.request("acme.identity.v1.UserService/UpdateUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/LintJournalEntries"), "journal_entry", "audit", "created_at"),
                ProbeSupport.value(sent1, "user", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "audit", "updated_at"),
                ProbeSupport.value(sent1, "user", "audit", "updated_at"));
        var sent2 = channel.request("acme.ledger.v1.PostingBatchNotificationService/SubscribePostingBatchUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "organizer", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/UpdateUser"), "user", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        var sent3 = channel.request("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "location", "region"),
                ProbeSupport.value(sent3, "metric_series", "address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/UpdateUser"), "user", "contact", "email"),
                ProbeSupport.value(response, "standing_order", "organizer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SubscribePostingBatchUpdates"), "posting_batch", "billing_address", "lines"),
                ProbeSupport.value(response, "standing_order", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order_id"),
                ProbeSupport.value(response, "standing_order", "standing_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "status"),
                ProbeSupport.value(response, "standing_order", "status"));
    }
}
