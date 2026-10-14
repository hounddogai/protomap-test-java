package com.acme.finance.domains.invoicing;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
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
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesRequest;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CountInvoiceTemplatesRequest;
import com.acme.invoicing.v1.CountInvoiceTemplatesResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationRequest;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationResponse;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.FindInvoiceTemplatesRequest;
import com.acme.invoicing.v1.FindInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceTemplateRequest;
import com.acme.invoicing.v1.LookupInvoiceTemplateResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesResponse;
import com.acme.lineage.v1.BatchCreateLineageNodesRequest;
import com.acme.lineage.v1.BatchCreateLineageNodesResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.lineage.v1.LineageNodeServiceGrpc;
import com.acme.modelregistry.v1.EstimateRegisteredModelRequest;
import com.acme.modelregistry.v1.EstimateRegisteredModelResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.ratelimiting.v1.FindRateLimitsRequest;
import com.acme.ratelimiting.v1.FindRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitQueryServiceGrpc;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class InvoiceTemplateQueryApplicationTest {
    @Test
    void exportInvoiceTemplatesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceTemplateQueryApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newFutureStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                LineageNodeServiceGrpc.newStub(channel),
                RegisteredModelCalculationServiceGrpc.newBlockingStub(channel),
                RateLimitQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportInvoiceTemplatesRequest.getDefaultInstance());
        var response = application.exportInvoiceTemplates(request);
        var sent0 = channel.request("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "bank_transfer", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "bank_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "bank_transfer", "lines", "attributes"));
        var sent1 = channel.request("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "bank", "bic"),
                ProbeSupport.value(sent1, "fx_hedge", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent1, "fx_hedge", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        var sent2 = channel.request("acme.invoicing.v1.EInvoiceCalculationService/ExplainEInvoiceCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "audit", "etag"),
                ProbeSupport.value(sent2, "e_invoice", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge"), "fx_hedge", "audit", "updated_by"),
                ProbeSupport.value(sent2, "e_invoice", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "bank_transfer", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "e_invoice", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/SimulateBankTransfer"), "breakdown", "total"),
                ProbeSupport.value(response, "invoice_template", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge"), "fx_hedge", "billing_address", "country_code"),
                ProbeSupport.value(response, "invoice_template", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge"), "fx_hedge", "audit", "created_at"),
                ProbeSupport.value(response, "invoice_template", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceCalculationService/ExplainEInvoiceCalculation"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "invoice_template", "billing_address", "region"));
        var endpoint = new InvoiceTemplateQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportInvoiceTemplatesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportInvoiceTemplatesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportInvoiceTemplates(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeInvoiceTemplatesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceTemplateQueryApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newFutureStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                LineageNodeServiceGrpc.newStub(channel),
                RegisteredModelCalculationServiceGrpc.newBlockingStub(channel),
                RateLimitQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeInvoiceTemplatesRequest.getDefaultInstance());
        var response = application.summarizeInvoiceTemplates(request);
        var sent0 = channel.request("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "risk_scenario", "requester", "email"));
        var sent1 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios"), "risk_scenario", "requester", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "counterparty", "email"),
                ProbeSupport.value(sent2, "invoice_template", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios"), "risk_scenario", "audit", "created_by"),
                ProbeSupport.value(sent2, "invoice_template", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "bank", "account_holder"),
                ProbeSupport.value(sent2, "invoice_template", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios"), "risk_scenario", "assignee", "display_name"),
                ProbeSupport.value(sent2, "invoice_template", "counterparty", "display_name"));
        var sent3 = channel.request("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility"), "invoice_template", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "billing_address", "region"),
                ProbeSupport.value(sent3, "bank_transfer", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility"), "invoice_template", "billing_address", "postal_code"),
                ProbeSupport.value(sent3, "bank_transfer", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferValidationService/ScoreBankTransfer"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
