package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScoreRiskRegisterRequest;
import com.acme.risk.v1.ScoreRiskRegisterResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.shows.v1.FindShowsRequest;
import com.acme.shows.v1.FindShowsResponse;
import com.acme.shows.v1.ShowQueryServiceGrpc;
import com.acme.telematics.v1.BrowseTelemetryReadingsRequest;
import com.acme.telematics.v1.BrowseTelemetryReadingsResponse;
import com.acme.telematics.v1.TelemetryReadingQueryServiceGrpc;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RiskRegisterValidationApplicationTest {
    @Test
    void normalizeRiskRegisterPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new RiskRegisterValidationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                new RiskRegisterValidationRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                ShowQueryServiceGrpc.newBlockingStub(channel),
                TelemetryReadingQueryServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(NormalizeRiskRegisterRequest.getDefaultInstance());
        var response = application.normalizeRiskRegister(request);
        var sent0 = channel.request("acme.banking.v1.MandateValidationService/PreviewMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "updated_at"),
                ProbeSupport.value(sent0, "mandate", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "etag"),
                ProbeSupport.value(sent0, "mandate", "audit", "etag"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "manager", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        var sent2 = channel.request("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent2, "fiscal_period", "participants", "contact", "email"));
        var sent3 = channel.request("acme.venues.v1.VenueValidationService/ValidateVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "manager", "user_id"),
                ProbeSupport.value(sent3, "venue", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport"), "fiscal_period", "audit", "updated_by"),
                ProbeSupport.value(sent3, "venue", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder"), "standing_order", "audit", "etag"),
                ProbeSupport.value(sent3, "venue", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "created_by"),
                ProbeSupport.value(sent3, "venue", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/PreviewMandate"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ValidateStandingOrder"), "standing_order", "location", "locality"),
                ProbeSupport.value(response, "risk_register", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/RetryFiscalPeriodExport"), "fiscal_period", "location", "lines"),
                ProbeSupport.value(response, "risk_register", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register_id"),
                ProbeSupport.value(response, "risk_register", "risk_register_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "parent_id"),
                ProbeSupport.value(response, "risk_register", "parent_id"));
    }
}
