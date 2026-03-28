# SchulNossa - Berlin Secondary School Finder
## Product Requirements Document (PRD)

**Version:** 1.0
**Last Updated:** January 2026
**Target Platform:** Web Application (Desktop & Mobile Responsive)
**Development Platform:** Lovable.dev / Similar AI-driven webapp builder

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Data Schema Reference](#2-data-schema-reference)
3. [User Personas](#3-user-personas)
4. [Feature Requirements](#4-feature-requirements)
5. [User Flows](#5-user-flows)
6. [UI/UX Specifications](#6-uiux-specifications)
7. [Technical Requirements](#7-technical-requirements)
8. [Implementation Task List](#8-implementation-task-list)

---

## 1. Executive Summary

### 1.1 Product Vision

SchulNossa is a premium web application that helps Berlin parents find the ideal secondary school (Gymnasium or ISS) for their children. The platform combines comprehensive school data with personalized family logistics to deliver intelligent recommendations.

### 1.2 Core Value Proposition

- **Data-Driven Decisions**: 259 Berlin secondary schools with 220+ data points each
- **Family-Centric Optimization**: Consider home, work, siblings' schools, and kitas
- **Smart Commute Analysis**: Real-time transit calculations via Google Maps
- **Personalized Scoring**: Weight factors that matter most to each family
- **Modern UX**: Real estate-style browsing with powerful filtering

### 1.3 Business Model

- **Trial Mode**: 5-minute free trial with progressive blur overlay
- **Paid Access**: 12-month subscription via Stripe
- **Pricing**: €49/year (suggested)

---

## 2. Data Schema Reference

### 2.1 School Master Table Overview

**Total Records:** 259 schools
**Total Columns:** 222 data points per school

### 2.2 Data Categories

#### 2.2.1 Basic School Information (15 fields)

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `schulnummer` | String | Official school ID | "01A04" |
| `schulname` | String | Full school name | "Schiller-Gymnasium" |
| `school_type` | Enum | School category | "Gymnasium", "ISS", "ISS-Gymnasium" |
| `schulart` | String | Detailed school type | "Integrierte Sekundarschule" |
| `traegerschaft` | String | Public/Private | "öffentlich", "privat" |
| `gruendungsjahr` | Integer | Year founded | 1897 |
| `strasse` | String | Street address | "Schillerstraße 125" |
| `plz` | String | Postal code | "10625" |
| `ortsteil` | String | Neighborhood | "Charlottenburg" |
| `bezirk` | String | District | "Charlottenburg-Wilmersdorf" |
| `telefon` | String | Phone number | "030 1234567" |
| `email` | String | Email address | "info@schiller-gym.de" |
| `website` | URL | School website | "https://schiller-gym.de" |
| `leitung` | String | Principal name | "Dr. Maria Schmidt" |
| `besonderheiten` | Text | Special features | "Musikprofil, MINT-Schwerpunkt" |

#### 2.2.2 Student & Teacher Statistics (6 fields)

| Field | Type | Description |
|-------|------|-------------|
| `schueler_2024_25` | Integer | Current student count |
| `lehrer_2024_25` | Integer | Current teacher count |
| `schueler_2023_24` | Integer | Previous year students |
| `lehrer_2023_24` | Integer | Previous year teachers |
| `schueler_2022_23` | Integer | Two years ago students |
| `lehrer_2022_23` | Integer | Two years ago teachers |

**Derived Metrics (calculate in app):**
- Student-to-teacher ratio
- Year-over-year growth trend
- School size category (small/medium/large)

#### 2.2.3 Academic Performance (8 fields)

| Field | Type | Description | Range |
|-------|------|-------------|-------|
| `abitur_durchschnitt_2024` | Float | Avg Abitur grade 2024 | 1.0-4.0 (lower=better) |
| `abitur_durchschnitt_2023` | Float | Avg Abitur grade 2023 | 1.0-4.0 |
| `abitur_durchschnitt_2025` | Float | Avg Abitur grade 2025 | 1.0-4.0 |
| `abitur_erfolgsquote_2024` | Float | Pass rate 2024 | 0-100% |
| `abitur_erfolgsquote_2025` | Float | Pass rate 2025 | 0-100% |
| `sprachen` | String | Languages offered | "EN, FR, ES, LA" |

**Note:** Only Gymnasiums and ISS with gymnasiale Oberstufe have Abitur data.

#### 2.2.4 Demand & Popularity (6 fields)

| Field | Type | Description |
|-------|------|-------------|
| `nachfrage_plaetze_2025_26` | Integer | Available spots |
| `nachfrage_wuensche_2025_26` | Integer | Applications received |
| `nachfrage_prozent_2025_26` | Float | Application ratio (>100% = oversubscribed) |
| `nachfrage_plaetze_2024_25` | Integer | Previous year spots |
| `nachfrage_wuensche_2024_25` | Integer | Previous year applications |

#### 2.2.5 Demographics (3 fields)

| Field | Type | Description |
|-------|------|-------------|
| `belastungsstufe` | Integer | Social challenge level (1-5) |
| `migration_2024_25` | Float | Migration background % |
| `migration_2023_24` | Float | Previous year migration % |

#### 2.2.6 Location (2 fields)

| Field | Type | Description |
|-------|------|-------------|
| `latitude` | Float | GPS latitude | 52.5200 |
| `longitude` | Float | GPS longitude | 13.4050 |

#### 2.2.7 Traffic & Safety Environment (16 fields)

| Field | Type | Description |
|-------|------|-------------|
| `plz_avg_cars_per_hour` | Float | Traffic volume |
| `plz_avg_bikes_per_hour` | Float | Bicycle traffic |
| `plz_avg_pedestrians_per_hour` | Float | Pedestrian traffic |
| `plz_avg_heavy_vehicles_per_hour` | Float | Truck traffic |
| `plz_avg_v85_speed` | Float | 85th percentile speed |
| `plz_bike_friendliness` | Float | Bike-friendly score |
| `plz_pedestrian_ratio` | Float | Pedestrian safety ratio |
| `plz_heavy_vehicle_ratio` | Float | Heavy vehicle ratio |
| `plz_speed_safe_zone` | Boolean | Safe speed zone |
| `plz_traffic_intensity` | String | Low/Medium/High |

#### 2.2.8 Crime Statistics (39 fields)

**Crime Categories (2023 & 2024 data + averages + YoY change):**
- Total crimes
- Robbery
- Street robbery
- Assault
- Aggravated assault
- Threats & coercion
- Bike theft
- Drug offenses
- Neighborhood crimes

| Key Derived Fields | Type | Description |
|-------------------|------|-------------|
| `crime_violent_crime_avg` | Float | Avg violent crimes |
| `crime_safety_rank` | Integer | Safety ranking (1=safest) |
| `crime_safety_category` | Enum | "safe", "moderate", "elevated" |

**Distribution:** 92 safe, 80 moderate, 87 elevated

#### 2.2.9 Public Transit (48 fields)

**For each transit type (rail, tram, bus) - 3 nearest stops:**

| Field Pattern | Type | Description |
|--------------|------|-------------|
| `transit_{type}_{01-03}_name` | String | Stop name |
| `transit_{type}_{01-03}_distance_m` | Integer | Distance in meters |
| `transit_{type}_{01-03}_latitude` | Float | Stop latitude |
| `transit_{type}_{01-03}_longitude` | Float | Stop longitude |
| `transit_{type}_{01-03}_lines` | String | Lines serving stop |

**Aggregate Fields:**

| Field | Type | Description | Range |
|-------|------|-------------|-------|
| `transit_stop_count_1000m` | Integer | Total stops within 1km | 0-50+ |
| `transit_all_lines_1000m` | String | All lines within 1km | "U7, S1, M10, 101" |
| `transit_accessibility_score` | Float | Overall transit score | 0-100 |

#### 2.2.10 Points of Interest (81 fields)

**POI Categories (with top 3 nearest for each):**

| Category | Count Field | Detail Fields |
|----------|-------------|---------------|
| Supermarkets | `poi_supermarket_count_500m` | name, address, distance_m, lat, lng |
| Restaurants | `poi_restaurant_count_500m` | name, address, distance_m, lat, lng |
| Bakery/Cafe | `poi_bakery_cafe_count_500m` | name, address, distance_m, lat, lng |
| Kitas | `poi_kita_count_500m` | name, address, distance_m, lat, lng |
| Primary Schools | `poi_primary_school_count_500m` | name, address, distance_m, lat, lng |
| Secondary Schools | `poi_secondary_school_count_500m` | — |

**POI Statistics:**
- Avg supermarkets within 500m: 6.7
- Avg restaurants within 500m: 11.9
- Avg bakery/cafes within 500m: 8.6
- Avg kitas within 500m: 4.3

### 2.3 School Type Distribution

| Type | Count | Description |
|------|-------|-------------|
| Gymnasium | 107 | Academic track leading to Abitur |
| ISS | 143 | Integrated secondary school |
| ISS-Gymnasium | 9 | Combined school with both tracks |

### 2.4 District Distribution

| District | Count |
|----------|-------|
| Charlottenburg-Wilmersdorf | 32 |
| Steglitz-Zehlendorf | 26 |
| Pankow | 25 |
| Mitte | 23 |
| Reinickendorf | 23 |
| Lichtenberg | 22 |
| Tempelhof-Schöneberg | 22 |
| Treptow-Köpenick | 19 |
| Marzahn-Hellersdorf | 18 |
| Neukölln | 18 |
| Spandau | 17 |
| Friedrichshain-Kreuzberg | 14 |

---

## 3. User Personas

### 3.1 Primary Persona: The Researching Parent

**Name:** Anna, 42
**Situation:** Mother of a 6th grader transitioning to secondary school
**Technical Comfort:** Moderate (uses apps daily)
**Key Needs:**
- Compare schools across multiple dimensions
- Understand commute implications for the family
- Make data-driven decision she can feel confident about

### 3.2 Secondary Persona: The Logistics Optimizer

**Name:** Marcus, 38
**Situation:** Father with two kids (one in Kita, one transitioning)
**Technical Comfort:** High
**Key Needs:**
- Optimize drop-off routes between Kita, Grundschule, and new school
- Minimize family commute time
- Find school near his workplace for emergencies

### 3.3 Tertiary Persona: The Quality Seeker

**Name:** Julia, 45
**Situation:** Parent prioritizing academic excellence
**Technical Comfort:** Moderate
**Key Needs:**
- Focus on Abitur grades and pass rates
- Compare only Gymnasiums
- Understand school reputation and demand

---

## 4. Feature Requirements

### 4.1 Authentication & Access Control

#### 4.1.1 Sign Up

**Fields:**
- Email address (required, validated)
- Password (min 8 chars, 1 uppercase, 1 number)
- First name (required)
- Accept terms & privacy policy (checkbox)

**Flow:**
1. User fills form
2. System sends verification email with 6-digit code
3. User enters code on verification screen
4. Account created, user redirected to onboarding

**UI Elements:**
- Clean, centered card layout
- Email field with real-time validation
- Password strength indicator
- "Already have an account? Sign in" link
- Social sign-in buttons (Google, Apple) - optional v2

#### 4.1.2 Sign In

**Fields:**
- Email address
- Password
- "Remember me" checkbox
- "Forgot password?" link

**Flow:**
1. User enters credentials
2. Validate against database
3. If valid, create session and redirect to dashboard
4. If invalid, show error (don't specify which field)

#### 4.1.3 Email Verification Code

**Specifications:**
- 6-digit numeric code
- Valid for 15 minutes
- Resend option after 60 seconds
- Max 3 resend attempts per hour

**UI:**
- 6 individual input boxes for code digits
- Auto-advance on digit entry
- Paste support for full code
- "Resend code" button with countdown timer

#### 4.1.4 Password Reset

**Flow:**
1. User clicks "Forgot password"
2. Enter email address
3. Receive email with reset link (valid 1 hour)
4. Click link, enter new password
5. Redirect to sign in

### 4.2 Trial Mode & Paywall

#### 4.2.1 Trial Mode (5 Minutes)

**Behavior:**
- Timer starts on first page load after sign-up
- Display subtle countdown in header: "Trial: 4:32 remaining"
- At 2 minutes: Show toast notification about trial ending
- At 1 minute: Increase countdown visibility (yellow background)
- At 30 seconds: Slight blur overlay begins (10% opacity)
- At 0: Progressive blur increases to 80% over 10 seconds

**Blur Overlay Specifications:**
```css
/* Progressive blur stages */
.blur-stage-1 { backdrop-filter: blur(2px); opacity: 0.3; }
.blur-stage-2 { backdrop-filter: blur(5px); opacity: 0.5; }
.blur-stage-3 { backdrop-filter: blur(10px); opacity: 0.7; }
.blur-stage-4 { backdrop-filter: blur(20px); opacity: 0.9; }
```

**Overlay Content:**
- Centered modal that appears through blur
- Headline: "Your free trial has ended"
- Subheadline: "Unlock unlimited access to find the perfect school"
- Price display: "€49/year - Less than the cost of a family dinner"
- CTA button: "Continue with Full Access"
- Benefits list:
  - Unlimited school comparisons
  - Smart commute calculations
  - Personalized school scoring
  - Save searches & favorites
  - School Finder Wizard

#### 4.2.2 Stripe Payment Integration

**Checkout Flow:**
1. User clicks "Continue with Full Access"
2. Redirect to Stripe Checkout (hosted page)
3. Stripe handles payment
4. Webhook receives confirmation
5. Update user record with subscription status
6. Redirect to dashboard with success message

**Stripe Configuration:**
- Product: "SchulNossa Annual Access"
- Price: €49.00 EUR
- Billing: One-time payment (yearly access)
- Success URL: `/dashboard?payment=success`
- Cancel URL: `/pricing?cancelled=true`

**User Status States:**
- `trial` - In 5-minute trial
- `trial_expired` - Trial ended, no payment
- `active` - Paid subscriber
- `expired` - Subscription ended (after 12 months)

### 4.3 User Profile & Settings

#### 4.3.1 Profile Settings

**Editable Fields:**
- First name
- Last name
- Email (with re-verification)
- Password change
- Language preference (DE/EN)
- Theme preference (Light/Dark/System)

#### 4.3.2 Personal Points of Interest (POIs)

**Purpose:** Allow users to save locations important to their family for commute calculations.

**POI Types:**
| Type | Icon | Description |
|------|------|-------------|
| Home | 🏠 | Family residence |
| Work (Parent 1) | 💼 | First parent's workplace |
| Work (Parent 2) | 💼 | Second parent's workplace |
| Kita | 👶 | Child's kindergarten |
| Grundschule | 🏫 | Sibling's primary school |
| Other School | 📚 | Other relevant school |
| Custom | 📍 | Any other location |

**POI Entry Form:**
- Label (e.g., "Papa's Office")
- Type (dropdown)
- Address (with Google Places Autocomplete)
- Geocoded lat/lng (stored automatically)

**Storage:**
- User can add up to 10 personal POIs
- Each POI stored with: label, type, address, lat, lng
- Stored in user profile in database

**Geocoding:**
- Use Google Geocoding API
- Trigger on address blur/selection
- Store coordinates for distance calculations

### 4.4 Dashboard (Main View)

#### 4.4.1 Layout Structure

```
┌─────────────────────────────────────────────────────────────────┐
│ HEADER: Logo | Search | Filters | User Menu | Theme Toggle     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐  │
│  │                     │  │                                 │  │
│  │    FILTERS PANEL    │  │         MAP VIEW                │  │
│  │    (Collapsible)    │  │    (Google Maps Embed)          │  │
│  │                     │  │                                 │  │
│  │  - School Type      │  │    • School markers             │  │
│  │  - District         │  │    • POI markers                │  │
│  │  - Abitur Range     │  │    • Personal POI markers       │  │
│  │  - Safety Level     │  │    • Cluster on zoom out        │  │
│  │  - Transit Score    │  │                                 │  │
│  │  - Languages        │  │                                 │  │
│  │  - etc.             │  │                                 │  │
│  │                     │  ├─────────────────────────────────┤  │
│  │                     │  │       SCHOOL CARDS GRID         │  │
│  │                     │  │                                 │  │
│  └─────────────────────┘  │  ┌─────┐ ┌─────┐ ┌─────┐       │  │
│                           │  │Card │ │Card │ │Card │       │  │
│                           │  └─────┘ └─────┘ └─────┘       │  │
│                           │  ┌─────┐ ┌─────┐ ┌─────┐       │  │
│                           │  │Card │ │Card │ │Card │       │  │
│                           │  └─────┘ └─────┘ └─────┘       │  │
│                           └─────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.4.2 View Toggles

- **Map + Cards** (default): Split view
- **Cards Only**: Full-width card grid
- **Map Only**: Full-width map with info popups
- **List View**: Table format with sortable columns

### 4.5 Filter System

#### 4.5.1 Filter Categories

**Basic Filters:**

| Filter | Type | Options |
|--------|------|---------|
| School Type | Multi-select | Gymnasium, ISS, ISS-Gymnasium |
| District | Multi-select | All 12 Berlin districts |
| Operator | Multi-select | Public, Private |

**Academic Filters:**

| Filter | Type | Options |
|--------|------|---------|
| Abitur Average | Range slider | 1.0 - 4.0 |
| Abitur Pass Rate | Range slider | 0% - 100% |
| Languages Offered | Multi-select | EN, FR, ES, LA, RU, TR, etc. |
| Has Abitur Data | Toggle | Yes/No |

**Safety & Environment:**

| Filter | Type | Options |
|--------|------|---------|
| Safety Category | Multi-select | Safe, Moderate, Elevated |
| Traffic Intensity | Multi-select | Low, Medium, High |
| Bike-Friendly Area | Toggle | Yes/No |

**Accessibility:**

| Filter | Type | Options |
|--------|------|---------|
| Transit Score | Range slider | 0 - 100 |
| Rail Station < 500m | Toggle | Yes/No |
| Bus Stop < 300m | Toggle | Yes/No |

**Demographics:**

| Filter | Type | Options |
|--------|------|---------|
| School Size | Multi-select | Small (<400), Medium, Large (>800) |
| Student-Teacher Ratio | Range slider | 5 - 25 |
| Challenge Level | Range slider | 1 - 5 |

**Demand & Competition:**

| Filter | Type | Options |
|--------|------|---------|
| Oversubscribed | Toggle | Yes/No |
| Application Ratio | Range slider | 0% - 300%+ |

**Nearby Amenities:**

| Filter | Type | Options |
|--------|------|---------|
| Supermarkets (500m) | Range slider | 0 - 20+ |
| Restaurants (500m) | Range slider | 0 - 20+ |
| Cafes/Bakeries (500m) | Range slider | 0 - 20+ |

#### 4.5.2 Filter Behavior

- Filters apply in real-time (debounced 300ms)
- Show count of matching schools: "Showing 47 of 259 schools"
- Clear all filters button
- Save filter preset option

### 4.6 School Cards

#### 4.6.1 Card Layout

```
┌────────────────────────────────────────────────────────────┐
│  [Pin Icon]                              [School Type Tag] │
│                                                            │
│  SCHOOL NAME                                               │
│  Neighborhood • District                                   │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ Key Metrics Row                                       │ │
│  │ 📊 2.3 Abitur  |  👥 850 Students  |  🚇 Transit: 78 │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ Safety: ●●●○○  |  Languages: EN, FR, LA             │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  [View Details]                    [Calculate Commute ▼]  │
└────────────────────────────────────────────────────────────┘
```

#### 4.6.2 Card Elements

**Header Section:**
- Pin/Favorite button (heart icon, top-left)
- School type badge (top-right)
  - Gymnasium: Blue badge
  - ISS: Green badge
  - ISS-Gymnasium: Purple badge

**Title Section:**
- School name (truncate with ellipsis if too long)
- Location subtitle: "Charlottenburg • Charlottenburg-Wilmersdorf"

**Metrics Row:**
- Abitur average (if available) with grade icon
- Student count with people icon
- Transit accessibility score with metro icon

**Secondary Info:**
- Safety indicator (5-dot scale, colored)
  - Safe: 5 green dots
  - Moderate: 3 yellow dots
  - Elevated: 1 red dot
- Languages offered (comma-separated)

**Actions:**
- "View Details" button → Opens detail modal/page
- "Calculate Commute" dropdown → Select personal POI → Show commute time

#### 4.6.3 Card States

- **Default**: Normal display
- **Hovered**: Slight elevation/shadow increase
- **Pinned**: Golden border, filled heart icon
- **Expanded**: Shows more metrics inline

### 4.7 School Detail View

#### 4.7.1 Detail Modal/Page Layout

**Triggered by:** Clicking "View Details" on card or map popup

**Sections:**

1. **Header**
   - School name (large)
   - Type badge
   - Pin/Favorite button
   - Share button
   - External link to school website

2. **Quick Stats Bar**
   - Abitur average
   - Student count
   - Teacher count
   - Founded year
   - Operator (public/private)

3. **Location & Map**
   - Mini Google Map showing school location
   - Address with copy button
   - Quick commute buttons for each saved POI

4. **Academic Performance Tab**
   - Abitur grades (3-year chart)
   - Pass rates (3-year chart)
   - Languages offered with flags
   - Special programs (from besonderheiten)

5. **Demand & Popularity Tab**
   - Application ratio visualization
   - Available spots vs applications
   - Trend indicator (increasing/decreasing demand)

6. **Safety & Environment Tab**
   - Safety category with explanation
   - Crime statistics summary
   - Traffic environment summary
   - Bike/pedestrian friendliness

7. **Transit & Accessibility Tab**
   - Transit score gauge
   - Nearest rail stations (with lines)
   - Nearest tram stops (with lines)
   - Nearest bus stops (with lines)
   - Interactive mini-map with transit stops

8. **Nearby Amenities Tab**
   - Supermarkets list (top 3)
   - Restaurants list (top 3)
   - Cafes/Bakeries list (top 3)
   - Nearby Kitas list (top 3)
   - Nearby schools list

9. **Contact & Links**
   - Phone (click to call)
   - Email (click to email)
   - Website (click to open)
   - Google Maps link

### 4.8 Map Features

#### 4.8.1 Map Configuration

**Base Map:**
- Google Maps JavaScript API
- Default center: Berlin (52.52, 13.405)
- Default zoom: 11 (city overview)
- Map style: Clean, muted colors (custom style JSON)

#### 4.8.2 Marker Types

| Type | Icon | Color | Size |
|------|------|-------|------|
| Gymnasium | 🎓 | Blue | Large |
| ISS | 📚 | Green | Large |
| ISS-Gymnasium | 🏫 | Purple | Large |
| Pinned School | ⭐ | Gold | Large |
| Personal POI - Home | 🏠 | Red | Medium |
| Personal POI - Work | 💼 | Orange | Medium |
| Personal POI - Kita | 👶 | Pink | Medium |
| Personal POI - School | 📕 | Teal | Medium |
| Transit Stop | 🚇 | Gray | Small |
| Supermarket | 🛒 | Light Blue | Small |
| Restaurant | 🍽️ | Brown | Small |

#### 4.8.3 Map Interactions

- **Click school marker**: Show info popup with mini-card
- **Click personal POI**: Show label and "Calculate routes" option
- **Hover**: Highlight marker, show name tooltip
- **Cluster**: When zoomed out, cluster nearby schools with count
- **Draw routes**: Show transit/driving routes between points

#### 4.8.4 Map Controls

- Zoom in/out buttons
- Fullscreen toggle
- Layer toggles:
  - Schools (on by default)
  - Personal POIs (on by default)
  - Transit stops (off by default)
  - Nearby amenities (off by default)
- Recenter on Berlin button

### 4.9 Saved Items

#### 4.9.1 Pinned Schools

**Functionality:**
- Click pin icon on any school card to save
- Pinned schools appear in dedicated "Favorites" section
- Persistent across sessions (stored in database)
- Maximum 20 pinned schools

**UI:**
- Pinned schools section on dashboard (collapsible)
- Pin count in header: "⭐ 5 Favorites"
- Remove pin option on each saved school

#### 4.9.2 Saved Searches

**Functionality:**
- Save current filter configuration with a name
- Reload saved searches instantly
- Share saved search via URL

**UI:**
- "Save Search" button in filter panel
- Modal to name the search
- Dropdown to load saved searches
- Manage saved searches in settings

### 4.10 Commute Calculator

#### 4.10.1 Single Commute Calculation

**Trigger:** "Calculate Commute" dropdown on school card

**Process:**
1. User selects a personal POI from dropdown
2. Call Google Distance Matrix API
3. Display results in popup/toast

**Results Display:**
```
┌─────────────────────────────────────┐
│ Commute from Home to Schiller-Gym  │
│                                     │
│ 🚇 Transit:  25 min (S+U)          │
│ 🚗 Driving:  15 min (5.2 km)       │
│ 🚴 Cycling:  18 min (4.8 km)       │
│ 🚶 Walking:  42 min (3.5 km)       │
│                                     │
│ [Show on Map]  [Add to Comparison] │
└─────────────────────────────────────┘
```

#### 4.10.2 Multi-Point Commute

**Use Case:** Calculate total family commute

**Flow:**
1. User selects school
2. User selects multiple POIs (e.g., Home + Work + Kita)
3. System calculates each leg
4. Display total family commute time

### 4.11 School Finder Wizard

#### 4.11.1 Wizard Purpose

A guided, step-by-step flow that helps users define their priorities and generates personalized school recommendations.

#### 4.11.2 Wizard Steps

**Step 1: Family Setup**
- How many children are transitioning? (1, 2, 3+)
- Current grade level? (5th, 6th)
- Do you have other children in school/kita? (Yes/No)

**Step 2: Location Setup**
- Enter home address (autocomplete)
- Enter work addresses (optional)
- Enter existing school/kita addresses (optional)

**Step 3: School Type Preference**
- What school type are you considering?
  - [ ] Gymnasium only
  - [ ] ISS only
  - [ ] Both types
- Operator preference?
  - [ ] Public only
  - [ ] Private only
  - [ ] Both

**Step 4: Commute Preferences**
- Maximum acceptable commute time for child? (slider: 10-60 min)
- Preferred transit mode?
  - [ ] Public transit
  - [ ] Cycling
  - [ ] Walking
  - [ ] Any
- Should school be near your work? (for emergencies)
  - Yes → Which work location?

**Step 5: Academic Priorities**
- How important is Abitur performance? (1-5 scale)
- Required languages?
  - [ ] English
  - [ ] French
  - [ ] Spanish
  - [ ] Latin
  - [ ] Other
- Special programs of interest?
  - [ ] MINT/STEM
  - [ ] Music
  - [ ] Arts
  - [ ] Sports
  - [ ] Bilingual

**Step 6: Environment Priorities**
- How important is neighborhood safety? (1-5 scale)
- How important is transit accessibility? (1-5 scale)
- How important are nearby amenities? (1-5 scale)
- School size preference?
  - [ ] Small (intimate community)
  - [ ] Medium (balanced)
  - [ ] Large (more opportunities)
  - [ ] No preference

**Step 7: Additional Factors**
- Avoid highly oversubscribed schools? (Yes/No)
- Prefer schools with low traffic in area? (Yes/No)
- Prefer schools near existing sibling school/kita? (Yes/No)

#### 4.11.3 Wizard Results

**Output:**
- Personalized ranked list of schools
- Score breakdown for each school
- Quick comparison view
- "Why this school?" explanation for top 3

**Scoring Algorithm:**
```
Total Score =
  (Commute Score × Commute Weight) +
  (Academic Score × Academic Weight) +
  (Safety Score × Safety Weight) +
  (Transit Score × Transit Weight) +
  (Amenity Score × Amenity Weight) +
  (Size Match Score × Size Weight) +
  (Language Match × Language Weight) +
  (Demand Feasibility Score × Demand Weight)
```

Weights derived from user's slider inputs in wizard.

#### 4.11.4 Wizard UI

- Progress indicator showing current step
- Back/Next navigation
- Skip option for optional steps
- Estimated time: "About 3 minutes"
- Save progress option

### 4.12 School Optimizer

#### 4.12.1 Purpose

Find the optimal school that minimizes total family commute while meeting quality criteria.

#### 4.12.2 Optimizer Inputs

**Required:**
- Home address (from personal POIs)
- At least one work address

**Optional:**
- Kita location
- Grundschule location (for sibling)
- Quality filters (Abitur threshold, safety, etc.)

#### 4.12.3 Optimization Algorithm

```
For each school meeting quality filters:
  total_time = 0

  // Morning route: Home → Kita → School → Work
  total_time += commute(home, kita)
  total_time += commute(kita, school)
  total_time += commute(school, work)

  // Afternoon route: Work → School → Kita → Home
  total_time += commute(work, school)
  total_time += commute(school, kita)
  total_time += commute(kita, home)

  school.optimization_score = total_time

Sort schools by optimization_score (ascending)
Return top 10
```

#### 4.12.4 Optimizer Output

- Ranked list of optimal schools
- Total daily commute time for each
- Interactive map showing optimal routes
- Comparison table

### 4.13 Comparison Tool

#### 4.13.1 Functionality

- Add up to 4 schools to comparison
- Side-by-side view of all metrics
- Highlight best/worst in each category
- Export comparison as PDF

#### 4.13.2 Comparison Table

| Metric | School A | School B | School C |
|--------|----------|----------|----------|
| Type | Gymnasium | ISS | Gymnasium |
| Abitur Avg | **2.1** | N/A | 2.4 |
| Students | 780 | **950** | 620 |
| Transit Score | 72 | **85** | 68 |
| Safety | Safe | Moderate | **Safe** |
| Commute (Home) | 22 min | **18 min** | 35 min |

Bold indicates best in category.

---

## 5. User Flows

### 5.1 New User Journey

```
Landing Page
    ↓
Sign Up Form
    ↓
Email Verification (6-digit code)
    ↓
Welcome Screen (brief tutorial)
    ↓
Trial Mode Begins (5 min countdown)
    ↓
Dashboard (explore freely)
    ↓
Trial Ends → Blur Overlay
    ↓
Stripe Checkout
    ↓
Payment Confirmation
    ↓
Full Access Dashboard
```

### 5.2 Returning User Journey

```
Landing Page
    ↓
Sign In Form
    ↓
Dashboard (based on subscription status)
    ↓
If trial_expired → Blur overlay
If active → Full access
```

### 5.3 School Discovery Flow

```
Dashboard
    ↓
Apply Filters (district, type, Abitur)
    ↓
Browse Cards/Map
    ↓
Click School Card
    ↓
View School Detail
    ↓
Pin School (or)
    ↓
Calculate Commute
    ↓
Add to Comparison
```

### 5.4 Wizard Flow

```
Dashboard
    ↓
Click "Find My Ideal School" button
    ↓
Wizard Step 1: Family Setup
    ↓
Wizard Step 2: Location Setup
    ↓
Wizard Step 3: School Type
    ↓
Wizard Step 4: Commute Preferences
    ↓
Wizard Step 5: Academic Priorities
    ↓
Wizard Step 6: Environment
    ↓
Wizard Step 7: Additional Factors
    ↓
Processing Animation
    ↓
Results: Ranked School List
    ↓
Explore Recommendations
```

### 5.5 Optimizer Flow

```
Dashboard → Optimizer Tab
    ↓
Review Personal POIs (add if missing)
    ↓
Set Quality Filters
    ↓
Click "Optimize"
    ↓
Processing Animation
    ↓
Results: Optimal Schools List
    ↓
View Route Visualization
    ↓
Explore/Compare Options
```

---

## 6. UI/UX Specifications

### 6.1 Design System

#### 6.1.1 Color Palette

**Light Mode:**

| Purpose | Color | Hex |
|---------|-------|-----|
| Primary | Royal Blue | #2563EB |
| Primary Hover | Darker Blue | #1D4ED8 |
| Secondary | Slate | #64748B |
| Success | Emerald | #10B981 |
| Warning | Amber | #F59E0B |
| Danger | Red | #EF4444 |
| Background | White | #FFFFFF |
| Surface | Light Gray | #F8FAFC |
| Border | Gray | #E2E8F0 |
| Text Primary | Dark Slate | #1E293B |
| Text Secondary | Slate | #64748B |

**Dark Mode:**

| Purpose | Color | Hex |
|---------|-------|-----|
| Primary | Light Blue | #3B82F6 |
| Primary Hover | Lighter Blue | #60A5FA |
| Secondary | Slate | #94A3B8 |
| Background | Dark Slate | #0F172A |
| Surface | Darker Slate | #1E293B |
| Border | Slate | #334155 |
| Text Primary | White | #F8FAFC |
| Text Secondary | Light Slate | #CBD5E1 |

#### 6.1.2 Typography

**Font Family:** Inter (Google Fonts)

| Element | Size | Weight | Line Height |
|---------|------|--------|-------------|
| H1 | 32px | 700 | 1.2 |
| H2 | 24px | 600 | 1.3 |
| H3 | 20px | 600 | 1.4 |
| H4 | 16px | 600 | 1.4 |
| Body | 14px | 400 | 1.5 |
| Small | 12px | 400 | 1.5 |
| Tiny | 10px | 400 | 1.4 |

#### 6.1.3 Spacing System

Base unit: 4px

| Token | Value |
|-------|-------|
| xs | 4px |
| sm | 8px |
| md | 16px |
| lg | 24px |
| xl | 32px |
| 2xl | 48px |
| 3xl | 64px |

#### 6.1.4 Border Radius

| Element | Radius |
|---------|--------|
| Buttons | 8px |
| Cards | 12px |
| Modals | 16px |
| Inputs | 6px |
| Badges | 4px |
| Full Round | 9999px |

#### 6.1.5 Shadows

```css
/* Light mode shadows */
--shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.05);
--shadow-md: 0 4px 6px rgba(0, 0, 0, 0.07);
--shadow-lg: 0 10px 15px rgba(0, 0, 0, 0.1);
--shadow-xl: 0 20px 25px rgba(0, 0, 0, 0.15);

/* Dark mode shadows */
--shadow-dark-sm: 0 1px 2px rgba(0, 0, 0, 0.3);
--shadow-dark-md: 0 4px 6px rgba(0, 0, 0, 0.4);
```

### 6.2 Component Specifications

#### 6.2.1 Buttons

**Primary Button:**
- Background: Primary color
- Text: White
- Padding: 12px 24px
- Border radius: 8px
- Hover: Darken 10%
- Active: Darken 15%
- Disabled: 50% opacity

**Secondary Button:**
- Background: Transparent
- Border: 1px solid primary
- Text: Primary color
- Hover: Light primary background

**Ghost Button:**
- Background: Transparent
- Text: Secondary color
- Hover: Light gray background

**Icon Button:**
- Size: 40px × 40px
- Border radius: 8px
- Icon centered

#### 6.2.2 Input Fields

- Height: 44px
- Padding: 12px 16px
- Border: 1px solid border color
- Border radius: 6px
- Focus: Primary color border, subtle shadow
- Error: Red border, error message below
- Placeholder: Secondary text color

#### 6.2.3 Cards

- Background: Surface color
- Border: 1px solid border color
- Border radius: 12px
- Padding: 16px
- Shadow: shadow-md
- Hover: Elevate with shadow-lg

#### 6.2.4 Badges/Tags

- Padding: 4px 8px
- Border radius: 4px
- Font size: 12px
- Font weight: 500

**Variants:**
- Primary: Blue background, white text
- Success: Green background, white text
- Warning: Amber background, dark text
- Danger: Red background, white text
- Neutral: Gray background, dark text

#### 6.2.5 Modals

- Overlay: Black at 50% opacity
- Modal background: Surface color
- Border radius: 16px
- Max width: 600px (responsive)
- Padding: 24px
- Close button: Top right corner

#### 6.2.6 Tooltips

- Background: Dark slate (inverted in dark mode)
- Text: White
- Padding: 8px 12px
- Border radius: 6px
- Max width: 250px
- Arrow pointing to trigger

### 6.3 Page Layouts

#### 6.3.1 Authentication Pages

```
┌─────────────────────────────────────────┐
│                                         │
│           SchulNossa Logo               │
│                                         │
│        ┌─────────────────────┐          │
│        │                     │          │
│        │    Form Card        │          │
│        │                     │          │
│        │    [Fields]         │          │
│        │                     │          │
│        │    [Button]         │          │
│        │                     │          │
│        └─────────────────────┘          │
│                                         │
│         Footer links                    │
└─────────────────────────────────────────┘
```

- Centered layout
- Gradient background (subtle)
- Card with form
- Responsive: Full width on mobile

#### 6.3.2 Dashboard Layout

**Desktop (>1024px):**
```
┌──────────────────────────────────────────────────────────────┐
│  Header (64px height)                                        │
├──────────────────────────────────────────────────────────────┤
│ ┌────────────┐ ┌──────────────────────────────────────────┐ │
│ │            │ │                                          │ │
│ │  Filters   │ │              Content Area                │ │
│ │   Panel    │ │                                          │ │
│ │  (280px)   │ │        (Map + Cards Grid)                │ │
│ │            │ │                                          │ │
│ │            │ │                                          │ │
│ └────────────┘ └──────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Tablet (768px - 1024px):**
- Collapsible filter panel (slide-in)
- Map and cards stack vertically

**Mobile (<768px):**
- Bottom sheet for filters
- Toggle between map and cards
- Single column card layout

### 6.4 Animations & Transitions

#### 6.4.1 Standard Transitions

```css
/* Default transition for interactive elements */
transition: all 0.2s ease-in-out;

/* Page transitions */
transition: opacity 0.3s ease, transform 0.3s ease;

/* Modal entrance */
@keyframes modal-enter {
  from {
    opacity: 0;
    transform: scale(0.95) translateY(10px);
  }
  to {
    opacity: 1;
    transform: scale(1) translateY(0);
  }
}
```

#### 6.4.2 Loading States

- **Skeleton loaders** for cards (gray pulsing rectangles)
- **Spinner** for buttons during API calls
- **Progress bar** for wizard steps
- **Shimmer effect** for content loading

#### 6.4.3 Micro-interactions

- Heart icon: Scale bounce on pin/unpin
- Cards: Subtle lift on hover
- Buttons: Scale down on click
- Checkboxes: Check mark draws in
- Toggle: Smooth slide transition

### 6.5 Responsive Breakpoints

| Name | Range | Layout |
|------|-------|--------|
| Mobile | 0 - 639px | Single column, bottom nav |
| Tablet | 640px - 1023px | Two columns, collapsible panels |
| Desktop | 1024px - 1279px | Full layout, fixed sidebar |
| Large Desktop | 1280px+ | Full layout, extra spacing |

### 6.6 Theme Toggle

**Behavior:**
- Toggle button in header (sun/moon icon)
- Three states: Light, Dark, System
- Preference saved in localStorage and user profile
- System: Follows OS preference

**Implementation:**
```css
/* Apply theme class to html element */
html.light { /* light mode variables */ }
html.dark { /* dark mode variables */ }

/* System preference */
@media (prefers-color-scheme: dark) {
  html.system { /* dark mode variables */ }
}
```

---

## 7. Technical Requirements

### 7.1 Frontend Stack

**Framework:** React 18+ with Next.js 14 (App Router)

**UI Library:**
- Tailwind CSS for styling
- shadcn/ui for component primitives
- Radix UI for accessible components

**State Management:**
- React Query (TanStack Query) for server state
- Zustand for client state

**Maps:**
- @react-google-maps/api
- Google Maps JavaScript API

**Forms:**
- React Hook Form
- Zod for validation

### 7.2 Backend Stack

**Option A: Serverless (Recommended for Lovable.dev)**
- Supabase for database + auth
- Supabase Edge Functions for API logic
- Supabase Storage for assets

**Option B: Traditional**
- Node.js + Express
- PostgreSQL database
- JWT authentication

### 7.3 Database Schema

#### 7.3.1 Users Table

```sql
CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email_verified BOOLEAN DEFAULT FALSE,
  verification_code VARCHAR(6),
  verification_expires_at TIMESTAMP,
  subscription_status VARCHAR(20) DEFAULT 'trial',
  trial_started_at TIMESTAMP,
  subscription_started_at TIMESTAMP,
  subscription_expires_at TIMESTAMP,
  stripe_customer_id VARCHAR(255),
  theme_preference VARCHAR(10) DEFAULT 'system',
  language VARCHAR(5) DEFAULT 'de',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);
```

#### 7.3.2 Personal POIs Table

```sql
CREATE TABLE personal_pois (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  label VARCHAR(100) NOT NULL,
  poi_type VARCHAR(50) NOT NULL,
  address TEXT NOT NULL,
  latitude DECIMAL(10, 8) NOT NULL,
  longitude DECIMAL(11, 8) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);
```

#### 7.3.3 Pinned Schools Table

```sql
CREATE TABLE pinned_schools (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  schulnummer VARCHAR(20) NOT NULL,
  pinned_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(user_id, schulnummer)
);
```

#### 7.3.4 Saved Searches Table

```sql
CREATE TABLE saved_searches (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  name VARCHAR(100) NOT NULL,
  filters JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);
```

#### 7.3.5 Schools Table

```sql
-- Import from CSV with all 222 columns
-- Primary key: schulnummer
-- Add indexes on: bezirk, school_type, latitude, longitude
```

### 7.4 API Endpoints

#### 7.4.1 Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/auth/signup | Create new user |
| POST | /api/auth/verify-email | Verify email code |
| POST | /api/auth/signin | Sign in user |
| POST | /api/auth/signout | Sign out user |
| POST | /api/auth/forgot-password | Request password reset |
| POST | /api/auth/reset-password | Reset password |

#### 7.4.2 User

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/user/profile | Get current user |
| PATCH | /api/user/profile | Update profile |
| GET | /api/user/subscription | Get subscription status |

#### 7.4.3 Personal POIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/pois | List user's POIs |
| POST | /api/pois | Add new POI |
| PATCH | /api/pois/:id | Update POI |
| DELETE | /api/pois/:id | Delete POI |

#### 7.4.4 Schools

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/schools | List schools (with filters) |
| GET | /api/schools/:id | Get school details |
| POST | /api/schools/pin/:id | Pin school |
| DELETE | /api/schools/pin/:id | Unpin school |
| GET | /api/schools/pinned | List pinned schools |

#### 7.4.5 Saved Searches

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/searches | List saved searches |
| POST | /api/searches | Save search |
| DELETE | /api/searches/:id | Delete search |

#### 7.4.6 Calculations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/calculate/commute | Calculate commute between points |
| POST | /api/calculate/optimize | Run optimizer algorithm |
| POST | /api/wizard/results | Process wizard and return recommendations |

### 7.5 External APIs

#### 7.5.1 Google Maps APIs

| API | Purpose | Pricing |
|-----|---------|---------|
| Maps JavaScript API | Map display | $7 per 1000 loads |
| Geocoding API | Address → coordinates | $5 per 1000 requests |
| Distance Matrix API | Commute calculations | $5 per 1000 elements |
| Places API | Address autocomplete | $2.83 per 1000 requests |
| Directions API | Route visualization | $5 per 1000 requests |

**Cost Estimation (per 1000 users/month):**
- Map loads: ~5000 = $35
- Geocoding: ~2000 = $10
- Distance Matrix: ~10000 = $50
- Places: ~5000 = $14
- Directions: ~2000 = $10
- **Total: ~$119/month per 1000 active users**

#### 7.5.2 Stripe API

| API | Purpose |
|-----|---------|
| Checkout Sessions | Create payment pages |
| Webhooks | Receive payment confirmations |
| Customer Portal | Manage subscriptions |

### 7.6 Security Requirements

- HTTPS everywhere
- Password hashing with bcrypt (cost factor 12)
- JWT tokens with 7-day expiry
- CSRF protection
- Rate limiting (100 requests/minute/IP)
- SQL injection prevention (parameterized queries)
- XSS prevention (sanitize all inputs)
- CORS configuration (allowed origins only)

### 7.7 Performance Requirements

- Initial page load: < 3 seconds
- Time to interactive: < 5 seconds
- API response time: < 500ms
- Map load time: < 2 seconds
- Filter application: < 300ms

---

## 8. Implementation Task List

### Phase 1: Foundation (Week 1-2)

#### 1.1 Project Setup
- [ ] Initialize Next.js project with TypeScript
- [ ] Configure Tailwind CSS
- [ ] Install and configure shadcn/ui
- [ ] Set up ESLint and Prettier
- [ ] Configure environment variables
- [ ] Set up Git repository

#### 1.2 Supabase Setup
- [ ] Create Supabase project
- [ ] Create users table with schema
- [ ] Create personal_pois table
- [ ] Create pinned_schools table
- [ ] Create saved_searches table
- [ ] Import schools data from CSV
- [ ] Set up Row Level Security policies
- [ ] Configure authentication settings

#### 1.3 Design System Implementation
- [ ] Create color palette CSS variables
- [ ] Implement light/dark theme system
- [ ] Create typography scale
- [ ] Create spacing system
- [ ] Build Button component variants
- [ ] Build Input component
- [ ] Build Card component
- [ ] Build Badge component
- [ ] Build Modal component
- [ ] Build Tooltip component
- [ ] Create loading skeletons

### Phase 2: Authentication (Week 2-3)

#### 2.1 Sign Up Flow
- [ ] Create sign up page layout
- [ ] Build sign up form with validation
- [ ] Implement email verification code sending
- [ ] Create verification code input UI (6 boxes)
- [ ] Implement verification code validation
- [ ] Handle sign up success/error states

#### 2.2 Sign In Flow
- [ ] Create sign in page layout
- [ ] Build sign in form with validation
- [ ] Implement authentication logic
- [ ] Create "Remember me" functionality
- [ ] Handle sign in success/error states

#### 2.3 Password Reset
- [ ] Create forgot password page
- [ ] Implement password reset email
- [ ] Create reset password page
- [ ] Implement password update logic

#### 2.4 Session Management
- [ ] Implement JWT token handling
- [ ] Create auth context provider
- [ ] Build protected route wrapper
- [ ] Handle session expiry

### Phase 3: Trial & Payment (Week 3-4)

#### 3.1 Trial Mode
- [ ] Implement trial timer (5 minutes)
- [ ] Create countdown display in header
- [ ] Build progressive blur overlay
- [ ] Create trial expiry modal
- [ ] Store trial state in database

#### 3.2 Stripe Integration
- [ ] Set up Stripe account and products
- [ ] Configure Stripe environment variables
- [ ] Create checkout session API endpoint
- [ ] Build checkout redirect logic
- [ ] Set up Stripe webhooks
- [ ] Handle payment success webhook
- [ ] Handle payment failure webhook
- [ ] Update user subscription status

#### 3.3 Subscription Management
- [ ] Create subscription status check
- [ ] Build subscription status UI
- [ ] Implement access control based on status
- [ ] Create subscription expired state

### Phase 4: Core Dashboard (Week 4-6)

#### 4.1 Layout Structure
- [ ] Create main dashboard layout
- [ ] Build responsive header
- [ ] Create collapsible filter sidebar
- [ ] Build main content area
- [ ] Implement responsive breakpoints

#### 4.2 School Cards
- [ ] Design and build school card component
- [ ] Implement pin/favorite functionality
- [ ] Add school type badges
- [ ] Display key metrics
- [ ] Add hover/active states
- [ ] Build card grid layout

#### 4.3 Filter System
- [ ] Build filter panel component
- [ ] Create multi-select filter (districts, types)
- [ ] Create range slider filter (Abitur, transit)
- [ ] Create toggle filter (boolean options)
- [ ] Implement filter state management
- [ ] Add filter count display
- [ ] Create "Clear all filters" function
- [ ] Implement real-time filter application

#### 4.4 School List/Grid
- [ ] Build school list container
- [ ] Implement sorting options
- [ ] Add pagination or infinite scroll
- [ ] Show "X of Y schools" count
- [ ] Handle empty state

### Phase 5: Map Integration (Week 6-7)

#### 5.1 Map Setup
- [ ] Configure Google Maps API
- [ ] Create map component wrapper
- [ ] Implement custom map styling
- [ ] Set default center and zoom

#### 5.2 Markers
- [ ] Create custom marker icons
- [ ] Implement school markers
- [ ] Implement personal POI markers
- [ ] Add marker clustering
- [ ] Build marker info popups

#### 5.3 Map Interactions
- [ ] Implement marker click handlers
- [ ] Add hover tooltips
- [ ] Sync map with filter results
- [ ] Implement "fly to" on card click
- [ ] Add layer toggle controls

#### 5.4 Route Visualization
- [ ] Implement directions API integration
- [ ] Draw transit routes on map
- [ ] Draw walking/cycling routes
- [ ] Show route in school detail view

### Phase 6: School Details (Week 7-8)

#### 6.1 Detail Modal/Page
- [ ] Create detail modal component
- [ ] Build header section
- [ ] Create quick stats bar
- [ ] Build location section with mini-map

#### 6.2 Detail Tabs
- [ ] Create tab navigation
- [ ] Build Academic Performance tab
- [ ] Build Demand & Popularity tab
- [ ] Build Safety & Environment tab
- [ ] Build Transit & Accessibility tab
- [ ] Build Nearby Amenities tab
- [ ] Build Contact & Links tab

#### 6.3 Detail Actions
- [ ] Implement pin from detail view
- [ ] Add share functionality
- [ ] Create "Calculate commute" integration
- [ ] Add to comparison from detail view

### Phase 7: Personal POIs (Week 8-9)

#### 7.1 POI Management
- [ ] Create POI settings page
- [ ] Build "Add POI" form
- [ ] Implement Google Places autocomplete
- [ ] Integrate geocoding API
- [ ] Display POI list with edit/delete
- [ ] Limit to 10 POIs per user

#### 7.2 POI on Map
- [ ] Display personal POIs on main map
- [ ] Create distinct POI marker styles
- [ ] Add POI labels on hover
- [ ] Build "show my POIs" toggle

### Phase 8: Commute Calculator (Week 9-10)

#### 8.1 Single Commute
- [ ] Create commute dropdown on cards
- [ ] Integrate Distance Matrix API
- [ ] Build commute results popup
- [ ] Display transit/driving/cycling/walking times
- [ ] Cache commute results

#### 8.2 Multi-Point Commute
- [ ] Build multi-point selection UI
- [ ] Calculate multi-leg journeys
- [ ] Display total family commute
- [ ] Show route breakdown

### Phase 9: Wizard & Optimizer (Week 10-12)

#### 9.1 School Finder Wizard
- [ ] Create wizard container with progress
- [ ] Build Step 1: Family Setup
- [ ] Build Step 2: Location Setup
- [ ] Build Step 3: School Type
- [ ] Build Step 4: Commute Preferences
- [ ] Build Step 5: Academic Priorities
- [ ] Build Step 6: Environment
- [ ] Build Step 7: Additional Factors
- [ ] Create processing animation
- [ ] Implement scoring algorithm
- [ ] Build results display
- [ ] Show "Why this school?" explanations

#### 9.2 Optimizer
- [ ] Create optimizer page/section
- [ ] Build POI selection UI
- [ ] Create quality filter presets
- [ ] Implement optimization algorithm
- [ ] Batch commute calculations
- [ ] Display ranked results
- [ ] Show optimal route visualization

### Phase 10: Saved Items & Comparison (Week 12-13)

#### 10.1 Pinned Schools
- [ ] Create favorites section on dashboard
- [ ] Implement pin/unpin logic
- [ ] Sync pins with database
- [ ] Add pin counter in header
- [ ] Build "manage favorites" view

#### 10.2 Saved Searches
- [ ] Create "Save search" modal
- [ ] Build saved searches list
- [ ] Implement load saved search
- [ ] Add delete saved search
- [ ] Generate shareable search URLs

#### 10.3 Comparison Tool
- [ ] Build comparison sidebar/drawer
- [ ] Create "Add to compare" action
- [ ] Limit to 4 schools
- [ ] Build comparison table view
- [ ] Highlight best/worst values
- [ ] Export comparison as PDF

### Phase 11: User Settings & Profile (Week 13-14)

#### 11.1 Profile Settings
- [ ] Create settings page layout
- [ ] Build profile edit form
- [ ] Implement email change with verification
- [ ] Create password change form
- [ ] Add theme preference setting
- [ ] Add language preference setting

#### 11.2 Subscription Management
- [ ] Display current subscription status
- [ ] Show subscription expiry date
- [ ] Link to Stripe customer portal
- [ ] Handle renewal reminders

### Phase 12: Polish & Optimization (Week 14-15)

#### 12.1 Performance
- [ ] Implement code splitting
- [ ] Add lazy loading for images
- [ ] Optimize map rendering
- [ ] Add caching for API responses
- [ ] Minimize bundle size

#### 12.2 Accessibility
- [ ] Add ARIA labels
- [ ] Ensure keyboard navigation
- [ ] Test with screen readers
- [ ] Check color contrast ratios

#### 12.3 Testing
- [ ] Write unit tests for core functions
- [ ] Write integration tests for flows
- [ ] Perform cross-browser testing
- [ ] Test on mobile devices

#### 12.4 Documentation
- [ ] Create user onboarding guide
- [ ] Write help documentation
- [ ] Create FAQ section

### Phase 13: Launch Preparation (Week 15-16)

#### 13.1 Final Polish
- [ ] Fix all known bugs
- [ ] Optimize for SEO
- [ ] Add analytics tracking
- [ ] Create error monitoring (Sentry)

#### 13.2 Deployment
- [ ] Set up production environment
- [ ] Configure CDN
- [ ] Set up SSL certificate
- [ ] Deploy application
- [ ] Configure domain

#### 13.3 Marketing
- [ ] Create landing page
- [ ] Write launch announcement
- [ ] Set up social media
- [ ] Prepare press kit

---

## Appendix A: Wireframe Sketches

### A.1 Landing Page

```
┌────────────────────────────────────────────────────────────────┐
│  Logo                                    Sign In | Sign Up     │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│           Find the Perfect Secondary School                    │
│                  for Your Child in Berlin                      │
│                                                                │
│     [      Search by neighborhood or school name      ]       │
│                                                                │
│            [Get Started - Free Trial]                         │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  259     │  │  Smart   │  │  Real-   │  │ Personl- │      │
│  │ Schools  │  │ Commute  │  │  time    │  │   ized   │      │
│  │          │  │ Calcultr │  │  Data    │  │  Scoring │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│              [Preview Map with School Dots]                    │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│                    Pricing: €49/year                           │
│                                                                │
│              [Start Free 5-Minute Trial]                       │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### A.2 Dashboard - Desktop

```
┌────────────────────────────────────────────────────────────────┐
│  Logo   [Search...]   ☆ 3   Trial: 4:32   👤 Anna   🌓       │
├────────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌─────────────────────────────────────────┐   │
│ │ FILTERS     │ │                                         │   │
│ │             │ │              GOOGLE MAP                 │   │
│ │ Type        │ │                                         │   │
│ │ [x] Gym     │ │     •    •       •                     │   │
│ │ [x] ISS     │ │        •    •        •                 │   │
│ │             │ │    •         •    •                    │   │
│ │ District    │ │         •        •                     │   │
│ │ [v] All     │ │                                         │   │
│ │             │ ├─────────────────────────────────────────┤   │
│ │ Abitur      │ │  Showing 47 of 259   [Grid] [List]     │   │
│ │ [====|====] │ ├─────────────────────────────────────────┤   │
│ │ 1.0    4.0  │ │ ┌─────────┐ ┌─────────┐ ┌─────────┐    │   │
│ │             │ │ │ Card 1  │ │ Card 2  │ │ Card 3  │    │   │
│ │ Safety      │ │ └─────────┘ └─────────┘ └─────────┘    │   │
│ │ [x] Safe    │ │ ┌─────────┐ ┌─────────┐ ┌─────────┐    │   │
│ │ [x] Mod.    │ │ │ Card 4  │ │ Card 5  │ │ Card 6  │    │   │
│ │ [ ] Elev.   │ │ └─────────┘ └─────────┘ └─────────┘    │   │
│ │             │ │                                         │   │
│ │ [Clear All] │ │                                         │   │
│ └─────────────┘ └─────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### A.3 School Card

```
┌─────────────────────────────────────────────────┐
│ ♡                                    [Gymnasium]│
│                                                 │
│  Friedrich-Schiller-Gymnasium                   │
│  Charlottenburg • Charlottenburg-Wilmersdorf    │
│                                                 │
│  📊 2.1 avg   👥 820   🚇 Transit: 85          │
│                                                 │
│  Safety: ●●●●●    Languages: EN, FR, LA, ES    │
│                                                 │
│  [View Details]              [Commute ▼]       │
└─────────────────────────────────────────────────┘
```

### A.4 Wizard Step

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  [●] [●] [○] [○] [○] [○] [○]     Step 2 of 7              │
│                                                             │
│                  Where does your family live?               │
│                                                             │
│  Home Address                                               │
│  ┌─────────────────────────────────────────────┐           │
│  │  Start typing your address...               │           │
│  └─────────────────────────────────────────────┘           │
│                                                             │
│  Work Address (Parent 1) - Optional                        │
│  ┌─────────────────────────────────────────────┐           │
│  │                                             │           │
│  └─────────────────────────────────────────────┘           │
│                                                             │
│  Work Address (Parent 2) - Optional                        │
│  ┌─────────────────────────────────────────────┐           │
│  │                                             │           │
│  └─────────────────────────────────────────────┘           │
│                                                             │
│                                                             │
│        [← Back]                        [Next →]            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Appendix B: Data Dictionary Quick Reference

| Column Prefix | Category | Count |
|---------------|----------|-------|
| (basic) | School Info | 15 |
| schueler_, lehrer_ | Statistics | 6 |
| abitur_ | Academic | 6 |
| nachfrage_ | Demand | 5 |
| migration_, belastungs_ | Demographics | 3 |
| lat, lng | Location | 2 |
| plz_ | Traffic/Safety | 16 |
| crime_ | Crime Stats | 39 |
| transit_ | Transit | 48 |
| poi_ | Points of Interest | 81 |
| **Total** | | **222** |

---

## Appendix C: Glossary

| Term | Definition |
|------|------------|
| **Abitur** | German high school diploma equivalent |
| **ISS** | Integrierte Sekundarschule (Integrated Secondary School) |
| **Gymnasium** | Academic-track secondary school |
| **Bezirk** | Berlin district |
| **Ortsteil** | Neighborhood within a district |
| **Kita** | Kindertagesstätte (daycare/kindergarten) |
| **Grundschule** | Primary/elementary school |
| **PLZ** | Postleitzahl (postal code) |
| **POI** | Point of Interest |
| **Schulnummer** | Official school identification number |

---

*End of Product Requirements Document*
