# Bundled content

A snapshot of SOC Prime content that a new installation starts with: Active Threats, their
Higher-Order Sigma (HoS) rules and Sigma detection rules. The ids are the real SOC Prime TDM
ids, so once the SOC Prime integration is connected, the regular sync keeps this content up to date.

The content in this folder is licensed under the [Detection Rule License (DRL) 1.1](LICENSE.Detection.Rules.md).

## Layout

```
VERSION                       # snapshot version; a new value is applied on the next start
active_threats/<tdm_id>.yml   # Active Threat; lists its HoS by TDM id
hos/<hos_uuid>.yml            # HoS rule; `body` is the HoS YAML, named by its `id`
rules/<sigma_uuid>.yml        # Sigma rule referenced by a HoS; `body` is the Sigma YAML
```

Each file has the shape of the matching SOC Prime TDM API object (`created` and `updated` are
the TDM timestamps, ISO 8601 with an offset). File names are for people only: ids are read from
the content.

## How it is applied

On every start the backend runs `python -m apps.modules.postgre.init_content`, which applies the
snapshot once per `VERSION`, with or without the SOC Prime integration:

- Active Threats, their HoS and the Sigma rules those HoS reference go to the Active Threats
  catalog and the **SOC Prime Active Threats** system repository, the same way a SOC Prime sync does.
- An item is written only when it is missing or newer than the local copy, and nothing is hidden
  or deleted, so content already synced from SOC Prime is never rolled back.

Connect the **SOC Prime Active Threats** repository to a pipeline for the rules to start matching.
