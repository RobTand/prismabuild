# Holders of the 171 held GiB on prismabuild-stage:dl380g10 (read-only, 2026-10-07 ~19:00Z)

Source: pb-queue/tier-reservations/prismabuild-stage:dl380g10/held (19 holder directories, one token file per GiB). Age is time since the oldest token file. State is the queue state of the holder row and of the consumer that registered its shared range.

| holder | GiB | age h | manifest | range GiB | holder row | consumer | consumer state | owner | basis |
|---|---|---|---|---|---|---|---|---|---|
| 5996ffb1b54b | 40 | 29.5 | f6df62948a57 | 0.0-40.0 | done | 4f5fe7566599 | failed | eng-pact-pricing | proved: consumer 4f5fe756 command capture_launch.py --root .../eng-pact-pricing-20261006 |
| 797cb37391bc | 40 | 446.7 | bd9def4f5629 | 0.0-40.0 | done | 3abe4a4a360a | failed | eng-pact-energy | proved: consumer 3abe4a4a command writes under .../eng-pact-energy-20261007/first-native-L03-01/gpu |
| ad9787e28fad | 40 | 29.6 | bd9def4f5629 | 40.0-80.0 | done | 3abe4a4a360a | failed | eng-pact-energy | proved: same consumer 3abe4a4a |
| d44016e19427 | 22 | 19.3 | bd9def4f5629 | 80.0-101.3 | done | 3abe4a4a360a | failed | eng-pact-energy | proved: same consumer 3abe4a4a |
| c2ea745cb049 | 8 | 29.6 | f6df62948a57 | 80.0-87.3 | done | 4f5fe7566599 | failed | eng-pact-pricing | proved: same consumer 4f5fe756 |
| 96706464064d | 7 | 19.9 | c2b3ee95fff8 | 0.0-6.6 | done | f0afae41b6e3 | done | D44 lane (eng-d44-*) | inferred: consumer command encode_launch.py --stage select-unit |
| 22773bc1bf42 | 2 | 446.5 | - | - | no queue row | - | no registration | unknown (glm campaign?) | inferred: no queue row, no registration; env PRISMABUILD_PRODUCED_SPOOL_ROOT=/home/rob/pb-spool/glm-campaign; runtime generation 05b5fe513815 |
| 28501a4b8ab9 | 1 | 19.9 | 5ba8d8be79f4 | 0.0-0.0 | done | 8ff507e6010f | done | pb-integrator | proved: canary leg 3 of my run 20261007T181252Zc134 |
| 42cae3588b75 | 1 | 19.9 | 5ba8d8be79f4 | 0.0-0.0 | done | 8ff507e6010f | done | pb-integrator | proved: canary leg 3 of my run 20261007T181252Zc134 |
| 554c204ef07e | 1 | 29.4 | 8ec6232171dd | 0.0-0.0 | done | dacb6414d259 | done | pb-integrator or the earlier canary driver | inferred: canary leg 3 command; consumer dacb6414 done, 29 h old |
| 69828b1074c1 | 1 | 19.9 | 3cf38cead690 | 0.0-0.0 | done | fca08d384734 | done | pb-integrator | proved: canary leg 3 of my run 20261007T182237Zc4 |
| 7b571f42035b | 1 | 19.9 | 46de21ab3af1 | 0.0-0.0 | done | 83480ca5fe6b | ready | kernels | inferred: consumer 83480ca5fe6b READY, command experiments/graph_attest_702, ORACLE_IMAGE |
| 86d65ceae760 | 1 | 29.4 | 680af31d319b | 0.0-0.3 | done | 8a778837e6b4 | failed | campaign (t8-nextarm source preparation) | inferred: consumer 8a778837 failed, stage1a_energy.py |
| 8e9adb3d77a6 | 1 | 29.4 | 8ec6232171dd | 0.0-0.0 | done | dacb6414d259 | done | pb-integrator or the earlier canary driver | inferred: canary leg 3 command; consumer dacb6414 done, 29 h old |
| a01df75a5e85 | 1 | 29.6 | b47fed9f607d | 0.0-0.0 | done | 1e7f5150fcc4 | ready | kernels | inferred: consumer 1e7f5150fcc4 READY, command experiments/t4_code/t4_fused_qualify_action.sh |
| a5d7cc85a3ad | 1 | 29.4 | 267398cdc289 | 0.0-0.3 | done | af0909e97017 | done | campaign (t8-nextarm source preparation) | inferred: consumer af0909e9 done, stage1a_energy.py |
| bd517f986da5 | 1 | 19.9 | 3cf38cead690 | 0.0-0.0 | done | fca08d384734 | done | pb-integrator | proved: canary leg 3 of my run 20261007T182237Zc4 |
| d433fc71c97b | 1 | 29.4 | 680af31d319b | 0.3-0.3 | done | 8a778837e6b4 | failed | campaign (t8-nextarm source preparation) | inferred: consumer 8a778837 failed, stage1a_energy.py |
| e33e50502949 | 1 | 2.9 | ecbf762f8ef9 | 0.0-0.0 | done | 7b1a0db8a5d2 | done | kernels | inferred: consumer 7b1a0db8a5d2 done, command experiments/t4_c... |

Total 171 GiB. Held by a consumer that is still READY or CLAIMED: 2 GiB. All other holders belong to consumers that are done or failed.
Not known: why the done and failed holders are still held. I did not read the tier loop release rules. I release nothing.
