package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.GetAttestationRulesRequest;
import com.acme.compliance.v1.GetAttestationRulesResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.NormalizeAttestationRequest;
import com.acme.compliance.v1.NormalizeAttestationResponse;
import com.acme.compliance.v1.PreviewAttestationRequest;
import com.acme.compliance.v1.PreviewAttestationResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
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
import org.springframework.stereotype.Service;

class AttestationValidationApplicationTest {
    @Test
    void validateAttestationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new AttestationValidationApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ValidateAttestationRequest.getDefaultInstance());
        var response = application.validateAttestation(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "bank", "bic"),
                ProbeSupport.value(sent0, "journal_entry", "bank", "bic"));
        var sent1 = channel.request("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry"), "journal_entry", "audit", "created_at"),
                ProbeSupport.value(response, "attestation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address"),
                ProbeSupport.value(response, "attestation", "billing_address"));
    }
}
