package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.CalculateAttestationRequest;
import com.acme.compliance.v1.CalculateAttestationResponse;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExplainAttestationCalculationRequest;
import com.acme.compliance.v1.ExplainAttestationCalculationResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetAttestationRatesRequest;
import com.acme.compliance.v1.GetAttestationRatesResponse;
import com.acme.compliance.v1.GetComplianceCaseRatesRequest;
import com.acme.compliance.v1.GetComplianceCaseRatesResponse;
import com.acme.compliance.v1.QuoteAttestationRequest;
import com.acme.compliance.v1.QuoteAttestationResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.SuggestDemandForecastsRequest;
import com.acme.forecasting.v1.SuggestDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.webhooks.v1.SummarizeWebhookEndpointsRequest;
import com.acme.webhooks.v1.SummarizeWebhookEndpointsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AttestationCalculationApplicationTest {
    @Test
    void simulateAttestationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new AttestationCalculationApplicationEvents(AuditTrailHistoryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AttestationCalculationApplication.EstimateAttestationStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AttestationCalculationApplication(
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                events,
                DemandForecastQueryServiceGrpc.newBlockingStub(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newFutureStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                new AttestationCalculationRequestMapperImpl(),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                StressTestCalculationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SimulateAttestationRequest.getDefaultInstance());
        var response = application.simulateAttestation(request);
        var sent0 = channel.request("acme.risk.v1.StressTestCalculationService/ExplainStressTestCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "lines", "amount"),
                ProbeSupport.value(sent0, "stress_test", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "lines", "line_id"),
                ProbeSupport.value(sent0, "stress_test", "lines", "line_id"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/ExplainStressTestCalculation"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent2, "compliance_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/ExplainStressTestCalculation"), "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        var sent3 = channel.request("acme.ledger.v1.JournalEntryQueryService/BrowseJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/ExplainStressTestCalculation"), "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions"), "compliance_case", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/BrowseJournalEntries"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "number"),
                ProbeSupport.value(response, "attestation", "number"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AttestationCalculationApplicationEvents(AuditTrailHistoryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CompareAuditTrailVersionsResponse>();
        listener.on(new AttestationCalculationApplication.EstimateAttestationStage1Requested(ProbeSupport.sample(CompareAuditTrailVersionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
