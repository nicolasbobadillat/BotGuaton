# Changelog: Banco de Chile Scraper

## [2026-02-21] - Extraction & Day Logic Fixes

### Added
- **Enhanced Discount Detection**: The scraper now attempts to pull the discount percentage directly from banner titles if the standard metadata is missing.
- **Expanded Antojo Detection**: Added automatic classification for `TACO BELL`, `VELVET BAKERY`, and `BURGERBEEF`.

### Fixed
- **"Bug de los Domingos"**: Corrected logic where specific day ranges starting or ending on Sunday (e.g., "Dom - Mié") were incorrectly triggering "Todos los días".
- **Dominga Bistro**: Fixed manual location mapping to Valdivia and specific day exclusion (Tuesday).

### Changed
- **Location Mapping**: Forced `location_id = 'all'` for all restaurants detected as 'Antojo' to maintain global project standards.
