# Changelog: BCI & Itaú Scrapers

## BCI Scraper [2026-02-18]
### Fixed
- **Premium Card Extraction**: Corrected extraction of card names from HTML metadata to properly differentiate between "Visa Signature", "Visa Infinite", and "Mastercard Black".
- **The Crust**: Verified mapping to `bci_premium` tier.

---

## Itaú Scraper [2026-02-17]
### Changed
- **Autonomous Location Mapping**: Refactored `itau.sql` transformer to map locations based on commune text without requiring manual overrides.
- **Commune Normalization**: Implemented regional suffix stripping (e.g., removing "Región de...") for better mapping accuracy.
### Fixed
- **Specific Sector Rules**: Added rules for San Joaquín, Peñuelas, and other sectors that were previously "ghost matching" to downtown communes.
