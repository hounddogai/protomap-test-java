package com.acme.finance.domains.treasury;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.ResolveAudiencesRequest;
import com.acme.audiences.v1.ResolveAudiencesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.SuggestAuditTrailsRequest;
import com.acme.audit.v1.SuggestAuditTrailsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.search.v1.ResolveSearchIndexesRequest;
import com.acme.search.v1.ResolveSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferLineTax;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckFxHedgeEligibilityRequest;
import com.acme.treasury.v1.CheckFxHedgeEligibilityResponse;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.GetFxHedgeRulesRequest;
import com.acme.treasury.v1.GetFxHedgeRulesResponse;
import com.acme.treasury.v1.LintFxHedgesRequest;
import com.acme.treasury.v1.LintFxHedgesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.NormalizeFxHedgeRequest;
import com.acme.treasury.v1.NormalizeFxHedgeResponse;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.PreviewFxHedgeRequest;
import com.acme.treasury.v1.PreviewFxHedgeResponse;
import com.acme.treasury.v1.ScoreFxHedgeRequest;
import com.acme.treasury.v1.ScoreFxHedgeResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.ValidateFxHedgeRequest;
import com.acme.treasury.v1.ValidateFxHedgeResponse;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.stereotype.Service;

class FxHedgeValidationApplicationTest {
    @Test
    void lintFxHedgesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeValidationApplication(
                AudienceQueryServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                StatementQueryServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(LintFxHedgesRequest.getDefaultInstance());
        var response = application.lintFxHedges(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "card", "token"),
                ProbeSupport.value(sent0, "cash_position", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "card", "network"),
                ProbeSupport.value(sent0, "cash_position", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        var sent1 = channel.request("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "variance", "requester", "preferred_channel"));
        var sent2 = channel.request("acme.treasury.v1.FxHedgeCalculationService/GetFxHedgeRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "fx_hedge_id"),
                ProbeSupport.value(sent2, "fx_hedge_id"));
        var sent3 = channel.request("acme.audit.v1.AuditEventService/UpdateAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "holder", "display_name"),
                ProbeSupport.value(sent3, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "customer", "number"),
                ProbeSupport.value(sent3, "audit_event", "subject", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "holder", "display_name"),
                ProbeSupport.value(response, "fx_hedge", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "fx_hedge", "counterparty", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/GetFxHedgeRates"), "fx_hedge", "number"),
                ProbeSupport.value(response, "fx_hedge", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/UpdateAuditEvent"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "due_at"),
                ProbeSupport.value(response, "fx_hedge", "due_at"));
    }
}
