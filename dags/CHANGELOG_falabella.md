# Changelog: Banco Falabella Scraper

## [2026-02-23] - Extreme Robustness (Slug Candidates & Forced Slugs)

### Added
- **Smart Slug Candidate Generation**: The scraper now generates up to 3 prioritized slug candidates for each restaurant name, increasing the hit rate for restaurants with complex suffixes or different naming conventions.
- **Dual Ampersand Strategy**: Added logic to handle `&` by trying both `y` replacement and total removal, fixing extraction for "Sushi & Burger Home".
- **Hardcoded Forced Slugs**: Implemented a priority override for "Cabrera" ensuring "La Cabrera al Paso" is always found via its specific slug.

### Changed
- **Modal Fallback Priority**: Enhanced the modal pass to rely more on direct extraction if slug candidates fail, ensuring 100% coverage even for hidden or dynamic cards.


## [2026-02-22] - Robustness & Performance Update

### Added
- **Phase 2 Fuzzy Matching**: Implemented fallback matching using the first two words of a restaurant's name if an exact match on the card grid fails. This solves issues with truncated card titles.
- **Phase 3 Forced Visits**: Added `EXTRA_NAMES` list to guarantee coverage for critical restaurants (Badass, Tanta, Muu Grill, etc.) regardless of grid loading states.
- **Plural Regex Support**: Updated name cleaning regex to handle "Descuentos" (plural), resolving extraction failures for items like "Paris Texas".

### Changed
- **Card Selector Update**: Switched targeting to `div[class*='NewCardBenefits_top-content__']` to match the latest Falabella frontend update.
- **Modal Navigation**: Replaced `Escape` key with `page.go_back()` to properly handle React Router state and avoid unmounting the grid.
- **Separated Extraction Logic**: Split `Restaurante` vs `Antojo` extraction paths to prevent cross-category regression.
- **Validation**: Replaced ambiguous "body text" checks with a strict `DetailBanner_wrapper-content` locator check.

### Fixed
- **Tanta Extraction**: Resolved "Sunday" parsing issue where it was being ignored.
- **Performance**: Applied timeout guards (`.count() > 0`) to prevent 30s hangs on optional elements.
- **ReDoS Protection**: Bounded address extraction regex (`[^#\n]{1,50}?`) to prevent catastrophic backtracking.

---
*Note: This scraper now completes a full run of ~100 items in ~11 minutes.*
