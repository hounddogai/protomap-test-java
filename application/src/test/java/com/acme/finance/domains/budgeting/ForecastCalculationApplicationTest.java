package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.budgeting.v1.SimulateForecastRequest;
import com.acme.budgeting.v1.SimulateForecastResponse;
import com.acme.budgeting.v1.ValidateBudgetRequest;
import com.acme.budgeting.v1.ValidateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.lending.v1.CountLoanApplicationsRequest;
import com.acme.lending.v1.CountLoanApplicationsResponse;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.seating.v1.CountSeatMapsRequest;
import com.acme.seating.v1.CountSeatMapsResponse;
import com.acme.ticketing.v1.ExportTicketsRequest;
import com.acme.ticketing.v1.ExportTicketsResponse;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ForecastCalculationApplicationTest {
    @Test
    void simulateForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new ForecastCalculationApplicationEvents(FeatureFlagValidationServiceGrpc.newStub(channel), LoanApplicationQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ForecastCalculationApplication.CompareForecastOptionsStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ForecastCalculationApplication.CompareForecastOptionsStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof ForecastCalculationApplication.RecalculateForecastsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ForecastCalculationApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                TicketQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SimulateForecastRequest.getDefaultInstance());
        var response = application.simulateForecast(request);
        var sent0 = channel.request("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "attachments", "content_type"),
                ProbeSupport.value(sent0, "mandate", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "attachments", "size_bytes"),
                ProbeSupport.value(sent0, "mandate", "attachments", "size_bytes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "attachments", "checksum"),
                ProbeSupport.value(sent0, "mandate", "attachments", "checksum"));
        var sent1 = channel.request("acme.seating.v1.SeatMapQueryService/CountSeatMaps");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetValidationService/ValidateBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "bank", "iban"),
                ProbeSupport.value(sent2, "budget", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "audit", "updated_by"),
                ProbeSupport.value(sent2, "budget", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "counterparty", "email"),
                ProbeSupport.value(sent2, "budget", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "audit", "created_by"),
                ProbeSupport.value(sent2, "budget", "audit", "created_by"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetValidationService/ScoreBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "bank", "account_holder"),
                ProbeSupport.value(sent3, "budget", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/ExplainMandateCalculation"), "mandate", "audit", "updated_at"),
                ProbeSupport.value(sent3, "budget", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "audit", "created_at"),
                ProbeSupport.value(sent3, "budget", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapQueryService/CountSeatMaps"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapQueryService/CountSeatMaps"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/ValidateBudget"), "budget", "holder", "display_name"),
                ProbeSupport.value(response, "forecast", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/ValidateBudget"), "budget", "audit", "created_at"),
                ProbeSupport.value(response, "forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/ScoreBudget"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast_id"),
                ProbeSupport.value(response, "forecast", "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ForecastCalculationApplicationEvents(FeatureFlagValidationServiceGrpc.newStub(channel), LoanApplicationQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckFeatureFlagEligibilityResponse>();
        listener.on(new ForecastCalculationApplication.CompareForecastOptionsStage0Requested(ProbeSupport.sample(CheckFeatureFlagEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CountLoanApplicationsResponse>();
        listener.on(new ForecastCalculationApplication.CompareForecastOptionsStage1Requested(ProbeSupport.sample(CountLoanApplicationsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<CountLoanApplicationsResponse>();
        listener.on(new ForecastCalculationApplication.RecalculateForecastsStage2Requested(ProbeSupport.sample(CountLoanApplicationsRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
