# Changelog: Scotiabank & Cencosud Scrapers

## Scotiabank [2026-02-13]
### Fixed
- **Kechua Multi-location**: Updated extraction logic to preserve and hash unique `offer_id`s for all physical locations (e.g., Vitacura, Providencia, Ñuñoa) instead of collapsing into one.

---

## Cencosud [2026-02-12]
### Fixed
- **Ari Nikkei Location Splitting**: Improved semicolon-delimited location parsing to accurately assign offers to "Las Condes" and "Lo Barnechea".
- **Valid Days Normalization**: Standardized "Lunes a Domingo" text output.
