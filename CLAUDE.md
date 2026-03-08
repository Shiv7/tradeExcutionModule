# Trade Execution Module -- Rules

## Adding a New Strategy
1. Add strategy key to `StrategyWalletResolver.ALL_STRATEGY_KEYS`
   Wallet auto-creates on next startup with 10,00,000 (10 Lakh)
2. Ensure signal consumer sets `signalSource` on every VirtualOrder
3. Update dashboard backend: `StrategyWalletsService.STRATEGY_KEYS` + `DISPLAY_NAMES` + `normalizeStrategy()`
4. Update dashboard frontend: `StrategyFilter` type + `STRATEGY_COLORS` + filter dropdown + skeleton

## Wallet Architecture
- Each strategy: `wallet:entity:strategy-wallet-{KEY}` in Redis
- Routing: `VirtualOrder.signalSource` -> `StrategyWalletResolver` -> wallet ID
- Fallback: orders without signalSource -> `virtual-wallet-1`
- Kill switch: `strategy.wallet.enabled=false`
- Initial capital per strategy: `strategy.wallet.initial.capital=1000000` (10 Lakh)

## Fund Allocation (FundAllocationService)
- Available fund = 50% of wallet's currentBalance (`fund.allocation.available.percent`)
- Deployable = available - usedMargin (already locked in open trades)
- **First batch window of day** (per exchange per strategy): proportional split across ALL signals by rankScore
- **Subsequent windows**: single winner (highest rankScore, OI tiebreaker) gets 50% of remaining deployable
- P&L recycled to wallet when trades close (VirtualEngineService -> WalletTransactionService.creditPnl)
- Daily state resets at midnight IST
- Applies to ALL strategies via centralized service

## Lot Size
- All qty calculations MUST use `LotSizeLookupService.roundToLotSize()`
- Data sources: `scripData` (MCX/Currency/Index) + `ScripGroup` (company-specific NSE F&O)
- Fallback lotSize=1 for equity. Log `ERR [LOT-SIZE]` if lookup fails.

## Display & Logging
- Missing data on frontend: show `DM`
- Errors in logs: prefix with `ERR [MODULE]`
- Errors on frontend: show `ERR` badge

## Build & Deploy
```bash
mvn clean package -DskipTests
sudo systemctl restart kotsin-trade-execution.service
journalctl -u kotsin-trade-execution.service -f --no-pager
```

## Key Files
- `FundAllocationService.java` -- Centralized fund allocation (proportional + single-winner)
- `StrategyWalletResolver.java` -- Strategy -> wallet mapping
- `LotSizeLookupService.java` -- Lot size + multiplier cache
- `VirtualEngineService.java` -- Order execution + wallet routing
- `WalletTransactionService.java` -- Margin + P&L operations
- `SignalBufferService.java` -- Unified signal batching (dynamic window: 3s opening/5s midday/2s post-NSE) + cross-strategy priority dedup (FUKAA>FUDKOI>FUDKII>MERE) + fund-allocated paper trade qty (lot-aware). No per-scrip buffer — all strategies go directly to shared batch.
- `RiskMonitorService.java` -- Per-wallet unrealized PnL monitoring
