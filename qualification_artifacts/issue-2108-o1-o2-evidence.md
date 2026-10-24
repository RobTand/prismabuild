# Issue #2108 O1/O2 evidence

## Scope and behavior

- Both GB10 roster entries explicitly select `tools/fleet/class_images.json`.
- The versioned policy declares `classes={}`: no production images, roots, or approvals are invented. Until approved requirements are supplied, container work remains unknown/ineligible (`class_requirements_missing`).
- The publisher validates roster-selected policies before staging and includes their bytes in both sealed fleet layouts via FLEET_DATA. Missing, unreadable, malformed and invalid-schema selections refuse publication.
- Worker loading failures retain an enabled, unknown/ineligible gate (`class_config_unavailable`) with no claimed requirements digest; native worker availability survives. The strict file loader and legacy unconfigured mode remain unchanged.
- No acceptance tests or launch helpers (`core`, `pool`, `docker`) were modified. No commit, activation, or fleet publication was performed.

## Observed checks

Initial acceptance run: 8 failed, demonstrating missing configuration/publication/failure behavior.
After implementation: 8 passed.
A broader run exposed 21 publisher regressions due to reduced test publications intentionally omitting the fleet roster. Preserved that behavior by skipping roster policy validation when fleet_boxes.json is excluded from FLEET_DATA.

Final command:

```sh
python -m pytest -q tests/test_container_class_requirements.py tests/test_publish_runtime.py tests/test_class_image_config_failure.py tests/test_class_image_fleet_generation.py --basetemp="$TMPDIR/pytest"
```

Result: **144 passed in 38.71s**, exit 0. `git diff --check` also passed. The full suite and live fleet deployment were not run.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"36d4627c8718730333d671570cf1bc61fdf0938f88d6641daea81b3a18505627","size":1810}},"operation":"8e9f1b6db1a5b275d1dc031a80e9f4253f3736b26f6e0ac272bdf9fbbaf57300","receipt":"7f376d8135d1e6bd12684cf84d846e090244f2d3e07c18ee9fa651a492f6aab2"}]
