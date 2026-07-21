# Changelog

## [0.2.0](https://github.com/DataIntegrationGroup/Aqueduct/compare/Aqueduct-v0.1.0...Aqueduct-v0.2.0) (2026-07-21)


### ⚠ BREAKING CHANGES

* move-credentials-to-adc-and-secret-manager-no-env-vars-or-keyfiles ([#15](https://github.com/DataIntegrationGroup/Aqueduct/issues/15))

### Features

* **frost:** persist FROST watermarks to GCS (ST2DAT-118) ([#17](https://github.com/DataIntegrationGroup/Aqueduct/issues/17)) ([9244e43](https://github.com/DataIntegrationGroup/Aqueduct/commit/9244e43a2fb6b74c45be9ae2da860698b25c41a4))


### Bug Fixes

* add missing comma in release-please-config.json ([#19](https://github.com/DataIntegrationGroup/Aqueduct/issues/19)) ([9989584](https://github.com/DataIntegrationGroup/Aqueduct/commit/9989584e9ea7b1281ed25d41e45264448ef47a3d))
* correct canonical model contract in HydroVu adapter(ST2DAT-197) ([#27](https://github.com/DataIntegrationGroup/Aqueduct/issues/27)) ([5224c00](https://github.com/DataIntegrationGroup/Aqueduct/commit/5224c00aae7fb9a840388d068f42ac17e6a63774))
* downgrade major changes before v1 ([#18](https://github.com/DataIntegrationGroup/Aqueduct/issues/18)) ([5415aef](https://github.com/DataIntegrationGroup/Aqueduct/commit/5415aefb18d17f48d7a9f4c92b8b608470650f94))
* **frost:** add request timeout and retry coverage to FROST API calls (ST2DAT-119) ([#20](https://github.com/DataIntegrationGroup/Aqueduct/issues/20)) ([5e5b2c2](https://github.com/DataIntegrationGroup/Aqueduct/commit/5e5b2c2f1816ef9d22cb1b0d7b9717214bc5a404))
* **hydrovu:** add date-partitioned GCS layout(ST2DAT-113) ([#12](https://github.com/DataIntegrationGroup/Aqueduct/issues/12)) ([45d043b](https://github.com/DataIntegrationGroup/Aqueduct/commit/45d043bfadc9c17916fbd4d49b97749e289830ec))
* **hydrovu:** fix silent error swallowing in fetch location data (ST2DAT-114) ([#16](https://github.com/DataIntegrationGroup/Aqueduct/issues/16)) ([24b4fc6](https://github.com/DataIntegrationGroup/Aqueduct/commit/24b4fc6d6b3f87647eccf050b0b2b690e17bcb64))
* **hydrovu:** move PVACD location IDs from code to config(ST2DAT-120) ([#14](https://github.com/DataIntegrationGroup/Aqueduct/issues/14)) ([94f6878](https://github.com/DataIntegrationGroup/Aqueduct/commit/94f68782fd6af44953079c9569e590e9398fa8c6))
* **hydrovu:** replace global cursor with per-location cursor (ST2DAT-115) ([#13](https://github.com/DataIntegrationGroup/Aqueduct/issues/13)) ([65960ea](https://github.com/DataIntegrationGroup/Aqueduct/commit/65960ea9fc2f6bcb1900efeda7e502833cb38809))


### Dependencies

* **dagster:** add dagster-cloud and dagster-dg-cli ([#10](https://github.com/DataIntegrationGroup/Aqueduct/issues/10)) ([2b033a2](https://github.com/DataIntegrationGroup/Aqueduct/commit/2b033a2da3eda86f2306c8f0aee68ca8eb8aa298))


### Documentation

* add backfill and initial start date strategy proposal(ST2DAT) ([#26](https://github.com/DataIntegrationGroup/Aqueduct/issues/26)) ([f04e011](https://github.com/DataIntegrationGroup/Aqueduct/commit/f04e01179208b1e46f4d643689215221fe72b7f0))
* add end-to-end pipeline documentation based on PVACD HydroVu  ([#30](https://github.com/DataIntegrationGroup/Aqueduct/issues/30)) ([c4ee021](https://github.com/DataIntegrationGroup/Aqueduct/commit/c4ee02121c3b748627f293e9d4e538fad88d4cce))
* add PVACD HydroVu source mapping and expand canonical model properties schema ST2DAT-182 ([#23](https://github.com/DataIntegrationGroup/Aqueduct/issues/23)) ([58fa66a](https://github.com/DataIntegrationGroup/Aqueduct/commit/58fa66ab9ebe5e05544279b51e086e1c9182efcc))
* add San Acacia source mapping(ST2DAT-147) ([#29](https://github.com/DataIntegrationGroup/Aqueduct/issues/29)) ([94f100b](https://github.com/DataIntegrationGroup/Aqueduct/commit/94f100b71701a2cf740c3e77210735d93f5cbaf6))
* add source mapping template md file for documentation migration… ([#21](https://github.com/DataIntegrationGroup/Aqueduct/issues/21)) ([db38656](https://github.com/DataIntegrationGroup/Aqueduct/commit/db386565be7f5e7c062191e47097533a7433c5af))
* **gcs:** add storage naming conventions and agent guidance ([#8](https://github.com/DataIntegrationGroup/Aqueduct/issues/8)) ([a45f77d](https://github.com/DataIntegrationGroup/Aqueduct/commit/a45f77d3dd12ee40ff7a2209b185e7d2128968d3))


### Build System

* move-credentials-to-adc-and-secret-manager-no-env-vars-or-keyfiles ([#15](https://github.com/DataIntegrationGroup/Aqueduct/issues/15)) ([762f28e](https://github.com/DataIntegrationGroup/Aqueduct/commit/762f28ea1fbcf3711680c0539ea69b566af8716f))

## 0.1.0 (2026-06-18)


### Bug Fixes

* fetch HydroVu location list once and add asset materialization metadata ([dbd3990](https://github.com/DataIntegrationGroup/Aqueduct/commit/dbd39909b50e83f905e960ec2324bd5a4e77e7da))
* fetch HydroVu location list once and add asset materialization metadata ([8074709](https://github.com/DataIntegrationGroup/Aqueduct/commit/8074709f1db918eef535d184d2240c8460b4b0d2))
* HydroVu pagination, incremental transform, and watermark correctness ([260501f](https://github.com/DataIntegrationGroup/Aqueduct/commit/260501fba4541ec7013018bffbdf5b2172cedde3))
* update start date and bucket ([a5aceaf](https://github.com/DataIntegrationGroup/Aqueduct/commit/a5aceafd5b395e833ed67eaa8e71fa2f426e21e0))


### Documentation

* add CANONICAL_MODEL.md to project structure ([3ecb778](https://github.com/DataIntegrationGroup/Aqueduct/commit/3ecb778d50b22b0c66460d57996c4d4d450748c1))
* update repo URL in readme ([8238cc1](https://github.com/DataIntegrationGroup/Aqueduct/commit/8238cc1b2e6906548090304e2533f0fd411bfb94))
