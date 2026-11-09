## Bundled Detection Content

A snapshot of SOC Prime content that a new installation starts with, covering the latest cyberattacks, built on 11 years of SOC Prime Detection Intelligence: Active Threats, their Higher Order Sigma rules, and the top-100 most effective behavior detection Sigma rules. The content is deployed automatically when you deploy Prime Detect.

## Layout

```
VERSION                       # snapshot version; a new value is applied on the next start
active_threats/<name>.yml     # Active Threat; lists its HoS by TDM id
hos/<name>.yml                # HoS rule; `body` is the HoS YAML
rules/<name>.yml              # Sigma rule referenced by a HoS; `body` is the Sigma YAML
```

Each file has the shape of the matching SOC Prime TDM API object (`created` and `updated` are
the TDM timestamps, ISO 8601 with an offset). File names are for people only: ids are read from
the content.

## How it is applied

On every start, the backend runs `python -m apps.modules.postgre.init_content`, which applies the
snapshot once per `VERSION`, with or without the SOC Prime integration:

- Active Threats, their HoS, and the Sigma rules those HoS reference go to the Active Threats
  catalog and the **SOC Prime Active Threats** system repository, the same way a SOC Prime sync does.
- An item is written only when it is missing or newer than the local copy, and nothing is hidden
  or deleted, so content already synced from SOC Prime is never rolled back.

Connect the **SOC Prime Active Threats** repository to a pipeline for the rules to start matching.
