package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.CountStatementsRequest;
import com.acme.banking.v1.CountStatementsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.boxoffice.v1.BoxOfficeSaleQueryServiceGrpc;
import com.acme.boxoffice.v1.FindBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.FindBoxOfficeSalesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.GetWriteOffRulesRequest;
import com.acme.collections.v1.GetWriteOffRulesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CheckUnderwritingEligibilityRequest;
import com.acme.insurance.v1.CheckUnderwritingEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.pipelines.v1.ExportPipelinesRequest;
import com.acme.pipelines.v1.ExportPipelinesResponse;
import com.acme.pipelines.v1.FindPipelinesRequest;
import com.acme.pipelines.v1.FindPipelinesResponse;
import com.acme.pipelines.v1.PipelineQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.sponsorships.v1.SimulateSponsorRequest;
import com.acme.sponsorships.v1.SimulateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.acme.venues.v1.CreateVenueRequest;
import com.acme.venues.v1.CreateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.stereotype.Service;

class WriteOffValidationApplicationTest {
    @Test
    void validateWriteOffPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var application = new WriteOffValidationApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StatementQueryServiceGrpc.newBlockingStub(channel),
                BoxOfficeSaleQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                new WriteOffValidationRequestMapperImpl(),
                PipelineQueryServiceGrpc.newBlockingStub(channel),
                PipelineQueryServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SponsorCalculationServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateWriteOffRequest.getDefaultInstance());
        var response = application.validateWriteOff(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyQueryService/SummarizePolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "metadata"),
                ProbeSupport.value(sent0, "details", "filters"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "card", "token"),
                ProbeSupport.value(sent1, "audit_trail", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "audit", "created_by"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "audit", "updated_at"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "billing_address", "locality"),
                ProbeSupport.value(sent1, "audit_trail", "billing_address", "locality"));
        var sent2 = channel.request("acme.documents.v1.DocumentQueryService/CountDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "counterparty", "collection_case_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.venues.v1.VenueService/CreateVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "billing_address", "locality"),
                ProbeSupport.value(sent3, "venue", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyQueryService/SummarizePolicies"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "issued_at"),
                ProbeSupport.value(response, "write_off", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentQueryService/CountDocuments"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueService/CreateVenue"), "venue", "owner", "display_name"),
                ProbeSupport.value(response, "write_off", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueService/CreateVenue"), "venue", "address", "lines"),
                ProbeSupport.value(response, "write_off", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off_id"),
                ProbeSupport.value(response, "write_off", "write_off_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "metadata"),
                ProbeSupport.value(response, "write_off", "metadata"));
    }
}
