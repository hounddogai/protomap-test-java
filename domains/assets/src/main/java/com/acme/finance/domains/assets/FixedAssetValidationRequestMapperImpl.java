package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.GetFixedAssetRulesRequest;
import com.acme.assets.v1.GetFixedAssetRulesResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.devices.v1.SimulateManagedDeviceRequest;
import com.acme.devices.v1.SimulateManagedDeviceResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class FixedAssetValidationRequestMapperImpl implements FixedAssetValidationRequestMapper {
    @Override
    public SimulateManagedDeviceRequest scoreFixedAssetStage0(ScoreFixedAssetRequest request) {
        return SimulateManagedDeviceRequest.newBuilder()
                .setContext(request.getContext())
                .setManagedDevice(ManagedDevice.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
    }
}
