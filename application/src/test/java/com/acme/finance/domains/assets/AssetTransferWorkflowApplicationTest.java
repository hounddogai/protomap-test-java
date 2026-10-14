package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceAssetTransferWorkflowRequest;
import com.acme.assets.v1.AdvanceAssetTransferWorkflowResponse;
import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CancelAssetTransferWorkflowRequest;
import com.acme.assets.v1.CancelAssetTransferWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.GenerateAssetTransferReportRequest;
import com.acme.assets.v1.GenerateAssetTransferReportResponse;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.InspectAssetTransferWorkflowRequest;
import com.acme.assets.v1.InspectAssetTransferWorkflowResponse;
import com.acme.assets.v1.ListAssetTransferWorkflowsRequest;
import com.acme.assets.v1.ListAssetTransferWorkflowsResponse;
import com.acme.assets.v1.PauseAssetTransferWorkflowRequest;
import com.acme.assets.v1.PauseAssetTransferWorkflowResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.RecallFixedAssetRequest;
import com.acme.assets.v1.RecallFixedAssetResponse;
import com.acme.assets.v1.ResumeAssetTransferWorkflowRequest;
import com.acme.assets.v1.ResumeAssetTransferWorkflowResponse;
import com.acme.assets.v1.SignalAssetTransferWorkflowRequest;
import com.acme.assets.v1.SignalAssetTransferWorkflowResponse;
import com.acme.assets.v1.StartAssetTransferWorkflowRequest;
import com.acme.assets.v1.StartAssetTransferWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.employees.v1.Employee;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.employees.v1.UpdateEmployeeRequest;
import com.acme.employees.v1.UpdateEmployeeResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.lending.v1.SignalLoanWorkflowRequest;
import com.acme.lending.v1.SignalLoanWorkflowResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.seating.v1.CloneSeatMapRequest;
import com.acme.seating.v1.CloneSeatMapResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapServiceGrpc;
import com.acme.sponsorships.v1.CreateSponsorRequest;
import com.acme.sponsorships.v1.CreateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorServiceGrpc;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class AssetTransferWorkflowApplicationTest {
    @Test
    void cancelAssetTransferWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new AssetTransferWorkflowApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                AssetTransferReportingServiceGrpc.newBlockingStub(channel),
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                EmployeeServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newFutureStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SeatMapServiceGrpc.newBlockingStub(channel),
                SponsorServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CancelAssetTransferWorkflowRequest.getDefaultInstance());
        var response = application.cancelAssetTransferWorkflow(request);
        var sent0 = channel.request("acme.seating.v1.SeatMapService/CloneSeatMap");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "seat_map", "children", "children", "location", "postal_code"));
        var sent1 = channel.request("acme.insurance.v1.UnderwritingImportService/MapUnderwritingImportColumns");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapService/CloneSeatMap"), "seat_map", "manager", "display_name"),
                ProbeSupport.value(sent1, "underwriting", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapService/CloneSeatMap"), "seat_map", "manager", "email"),
                ProbeSupport.value(sent1, "underwriting", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "audit", "created_at"),
                ProbeSupport.value(sent1, "underwriting", "comments", "created_at"));
        var sent2 = channel.request("acme.insurance.v1.CoverageApprovalService/EscalateCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapService/CloneSeatMap"), "seat_map", "manager", "user_id"),
                ProbeSupport.value(sent2, "details", "approver", "user_id"));
        var sent3 = channel.request("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/MapUnderwritingImportColumns"), "underwriting", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapService/CloneSeatMap"), "seat_map", "manager", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.seating.v1.SeatMapService/CloneSeatMap"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/MapUnderwritingImportColumns"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/EscalateCoverage"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry"), "journal_entry", "audit", "created_at"),
                ProbeSupport.value(response, "asset_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/NormalizeJournalEntry"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer_id"),
                ProbeSupport.value(response, "asset_transfer", "asset_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "status"),
                ProbeSupport.value(response, "asset_transfer", "status"));
    }
}
