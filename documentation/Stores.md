# Stores And Source Mapping

This document intentionally excludes credentials and tokenized URLs.
Use it to map chains to source systems and runtime brand folders.

## Security policy

- Do not commit API keys, access tokens, or full tokenized request URLs.
- Keep secrets in local environment variables, secret managers, or CI secrets.
- Rotate any credentials that were ever committed to git history.

## Sobeys portfolio (Flipp)

- sobeys
- safeway
- iga
- freshco
- foodland
- longos
- farm_boy

## Loblaws portfolio (Flipp)

- loblaws
- nofrills
- real_canadian_superstore
- provigo
- maxi
- zehrs
- fortinos
- atlantic_superstore
- dominion
- independent_grocer
- independent_city_market
- freshmart

## Walmart portfolio (Flipp)

- walmart (with grocery flyer type filter)

## Metro portfolio (Metro Digital API)

- metro
- metro_qc
- food_basics
- super_c
- adonis

Base API: `https://metrodigital-apim.azure-api.net/api`

## Runtime source of credentials

- Flipp brands: brand-level access token fields in fetcher configuration.
- Metro brands: banner/API key from fetcher configuration or app-config discovery.

For endpoint and payload details, see `documentation/METRO_API.md`.

