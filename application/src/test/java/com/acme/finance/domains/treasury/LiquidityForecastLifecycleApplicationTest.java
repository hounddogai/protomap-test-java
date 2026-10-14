package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.customers.v1.CompareConsentRecordOptionsRequest;
import com.acme.customers.v1.CompareConsentRecordOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.SimulateCustomerRequest;
import com.acme.customers.v1.SimulateCustomerResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanApplicationWorkflowRequest;
import com.acme.lending.v1.PauseLoanApplicationWorkflowResponse;
import com.acme.ratelimiting.v1.CalculateRateLimitRequest;
import com.acme.ratelimiting.v1.CalculateRateLimitResponse;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitBlockAction;
import com.acme.ratelimiting.v1.RateLimitCalculationServiceGrpc;
import com.acme.treasury.v1.ActivateLiquidityForecastRequest;
import com.acme.treasury.v1.ActivateLiquidityForecastResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireLiquidityForecastRequest;
import com.acme.treasury.v1.ExpireLiquidityForecastResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleRequest;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastBankSettlement;
import com.acme.treasury.v1.LiquidityForecastReportingServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.ReinstateLiquidityForecastRequest;
import com.acme.treasury.v1.ReinstateLiquidityForecastResponse;
import com.acme.treasury.v1.RenewLiquidityForecastRequest;
import com.acme.treasury.v1.RenewLiquidityForecastResponse;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import com.acme.treasury.v1.SuspendLiquidityForecastRequest;
import com.acme.treasury.v1.SuspendLiquidityForecastResponse;
import com.acme.treasury.v1.TerminateLiquidityForecastRequest;
import com.acme.treasury.v1.TerminateLiquidityForecastResponse;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class LiquidityForecastLifecycleApplicationTest {
    @Test
    void reinstateLiquidityForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new LiquidityForecastLifecycleApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                RenewalWorkflowServiceGrpc.newFutureStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                RateLimitCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastReportingServiceGrpc.newBlockingStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ReinstateLiquidityForecastRequest.getDefaultInstance());
        var response = application.reinstateLiquidityForecast(request);
        var sent0 = channel.request("acme.ratelimiting.v1.RateLimitCalculationService/CalculateRateLimit");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "rate_limit", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent0, "rate_limit", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "rate_limit", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "rate_limit", "block", "notify_email"));
        var sent1 = channel.request("acme.lending.v1.LoanApplicationWorkflowService/PauseLoanApplicationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent1, "loan_application", "counterparty", "display_name"));
        var sent2 = channel.request("acme.customers.v1.CustomerCalculationService/SimulateCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/PauseLoanApplicationWorkflow"), "loan_application", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "customer", "primary_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent2, "customer", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/CalculateRateLimit"), "rate_limit", "audit", "updated_by"),
                ProbeSupport.value(sent2, "customer", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/CalculateRateLimit"), "rate_limit", "owner", "email"),
                ProbeSupport.value(sent2, "customer", "relationship_manager", "email"));
        var sent3 = channel.request("acme.contracts.v1.RenewalWorkflowService/ResumeRenewalWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent3, "renewal", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/SimulateCustomer"), "customer", "contact", "email"),
                ProbeSupport.value(sent3, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/PauseLoanApplicationWorkflow"), "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent3, "renewal", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/CalculateRateLimit"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ratelimiting.v1.RateLimitCalculationService/CalculateRateLimit"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/PauseLoanApplicationWorkflow"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/PauseLoanApplicationWorkflow"), "loan_application", "billing_address", "locality"),
                ProbeSupport.value(response, "liquidity_forecast", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/SimulateCustomer"), "breakdown", "total"),
                ProbeSupport.value(response, "liquidity_forecast", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalWorkflowService/ResumeRenewalWorkflow"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
