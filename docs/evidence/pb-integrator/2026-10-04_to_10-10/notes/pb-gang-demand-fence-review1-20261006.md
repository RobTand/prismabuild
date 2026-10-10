# PR 1584 review 1 (REQUEST_CHANGES at e9bc0f89): what I propose
Review: https://github.com/RobTand/prismabuild/pull/1584#issuecomment-6022179031

1. Prerequisite slack (design hole, real). Stage movers are priced at up to `readers` CPUs (default 4) and several GiB
   (`storage_tiers.mover_demand_from_receipts`), and the stage tier is on sparklina, so movers run on a gang member's host.
   A member that takes all CPUs leaves no slack and a mover its member waits for is held: the same cycle that killed 1580.
2. `returns_capacity` can be sealed by any action, so a consuming action could bypass the reservation (trust gap).
3. A carried measurement withhold skips the new precedence guard (code defect, mine; I fix it).
4. A persisted HIGHER-priority waiting-measurement election stops an aged gang member from electing.

## Proposal
(1) Declare residency movers the same way returners are declared: a sealed `serves_residency: true` on stage movers and RAM
promotions (the pbrun movers, `produced_output` movers), copied onto the row by `publish`. Such a row skips the reservation
arithmetic but still needs its real ledger fit (tier tokens, CPU, memory), so it cannot take what is not free. Cost: a stream
of unrelated movers on a member host can delay the gang by one mover run each; movers are finite and token-bounded, and the
limit is documented. Alternative: reserve the member demand minus one mover's declared width. Rejected, because unrelated
small rows fill that allowance first and the prerequisite is held again.
(2) Bound what may declare an exemption at `publish`: a `returns_capacity` row must demand at most cpu 1 and mem_gb 1, no gpu,
and otherwise only tier-token kinds; a `serves_residency` row must carry a residency range block and no gpu. Anything else is
refused with PoolContractError. A consuming action then cannot use the flag.
(3) Fix: suspend the measurement withhold on the carried-withhold path too, with a test.
(4) Keep priority semantics: a strictly higher-priority measurement election still fences the gang (higher priority wins by
design, and it ends when that measurement runs). "Reservation wins" applies to measurement rows at the gang's priority or
lower. D45 puts gangs at priority 0, so only a priority-10 ship-window measurement outranks them. Documented, with a test.
