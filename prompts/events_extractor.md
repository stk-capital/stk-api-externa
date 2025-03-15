# Event Extractor

## About
The Event Extractor identifies financial and corporate events from a block of text, providing structured information about each event including the name, date, companies involved, and other relevant details.

## Goal
Extract structured event information from financial text, providing:
1. Event identification and categorization
2. Complete event details (name, date, location, etc.)
3. Normalized date representation
4. Relevance filtering
5. Confidence scoring
6. Deduplication handling

## Input Schema
```json
{
    "text": "The input text to analyze for events",
    "candidates": [
        {
            "id": "event-id-1",
            "name": "Q3 2023 Earnings Call",
            "description": "Company XYZ will present its Q3 2023 financial results",
            "date": "2023-11-15T14:30:00Z",
            "original_date_text": "November 15, 2023 at 2:30 PM EST",
            "location": "Virtual Conference",
            "event_type": "earnings_call",
            "companies_ids": ["company-id-1"],
            "confidence": 0.95
        }
    ]
}
```

## Output Schema
```json
[
    {
        "id": "event-id-1",
        "name": "Q3 2023 Earnings Call",
        "description": "Company XYZ will present its Q3 2023 financial results",
        "date": "2023-11-15T14:30:00-05:00",
        "original_date_text": "November 15, 2023 at 2:30 PM EST",
        "location": "Virtual Conference",
        "event_type": "earnings_call",
        "companies": ["XYZ Inc."],
        "confirmed": true,
        "confidence": 0.95,
        "already_exists": false,
        "needs_update": false
    }
]
```

## Instructions

### Event Identification
1. Identify all distinct events mentioned in the input text.
2. Look for key indicators of events such as:
   - Announcements of upcoming meetings, calls, or conferences
   - Scheduled financial releases or disclosures
   - Corporate actions and decisions
   - Regulatory events or deadlines
   - Product launches or releases
3. Only extract events that have clear date information (either exact or approximate).
4. If multiple events are mentioned, extract each as a separate entity.

### Event Details
For each event, extract:

1. **Name**: A concise, descriptive title for the event
   - Should be specific and clearly identify the event type
   - Include company name if it's company-specific
   - Example: "Apple Q2 2023 Earnings Conference Call"

2. **Description**: A brief summary of the event purpose
   - Should provide context beyond what's in the name
   - Include key details about what will be presented/discussed
   - Example: "Apple will discuss its Q2 2023 financial results and business outlook"

3. **Date Information**: The original text describing when the event will occur
   - Extract the exact date text as mentioned
   - Include time zone information if provided
   - Example: "May 4, 2023, at 2:00 p.m. PT"

4. **Location**: Where the event will take place
   - Include physical location or specify if virtual/webcast
   - Include relevant access information if provided
   - Example: "Virtual webcast" or "Company Headquarters, Cupertino, CA"

5. **Event Type**: Categorize the event into one of these standardized types:
   - `earnings_call`: Financial results presentations, quarterly earnings reports, or financial performance calls
   - `investor_conference`: Investor-focused events, industry conferences, or financial forums
   - `annual_meeting`: Annual shareholder meetings or annual general meetings (AGMs)
   - `special_meeting`: Special shareholder meetings for specific votes or decisions
   - `regulatory_filing`: Important filing deadlines, submissions to regulatory bodies, or compliance events
   - `merger_acquisition`: M&A activities, takeovers, buyouts, or significant partnership announcements
   - `product_launch`: New product or service announcements, releases, or unveilings
   - `executive_change`: Leadership transitions, executive appointments, departures, or board changes
   - `dividend_event`: Dividend announcements, payments, changes, or special dividends
   - `analyst_day`: Analyst days, investor days, or capital markets days
   - `trade_show`: Industry exhibitions, trade shows, or expos
   - `capital_markets`: Stock offerings, debt issuances, stock splits, or capital raising events
   - `investor_roadshow`: Pre-IPO or financing roadshows, investor tours, or promotional events
   - `ipo_direct_listing`: Initial public offerings, direct listings, or public market debuts
   - `stock_related`: Stock buybacks, splits, consolidations, or other equity-related events
   - `facility_event`: Facility openings, closings, expansions, or site visits
   - `corporate_social`: Corporate social responsibility events, charity events, or sustainability initiatives
   - `legal_regulatory`: Legal proceedings, settlements, regulatory decisions, or compliance announcements
   - `award_recognition`: Industry awards, recognitions, rankings, or milestone celebrations
   - `corporate_restructuring`: Business restructuring, reorganizations, spinoffs, or division closures
   - `strategic_update`: Business strategy updates, long-term plan announcements, or outlook presentations
   - `other`: Any event that doesn't fit into the above categories

6. **Companies**: List of company names mentioned in relation to the event
   - Include all companies that are hosting or directly participating
   - Do not include peripheral companies just mentioned in the text

7. **Confirmed**: Boolean indicating if the event is confirmed to occur
   - Set to `true` for definitely scheduled events
   - Set to `false` for tentative, rumored, or potential events

8. **Confidence**: Score (0.0-1.0) indicating confidence in the extraction
   - Higher for clearly defined events with complete information
   - Lower for events with vague, incomplete, or ambiguous details
   - Consider factors like clarity of date, specificity of description, etc.

### Event Deduplication and Update
When extracting events, consider the candidate events provided in the input to handle deduplication and updates:

1. **Identifying Existing Events**:
   - Compare extracted events with candidate events from the input
   - Consider an event a potential match if it has:
     - Similar name/title (semantic similarity)
     - Same company involvement
     - Similar date (within a reasonable window)
   - For matching events, set `already_exists` to `true` in the output
   - For matching events, include the original `id` from the candidate in the output
   - For new events not matching any candidates, set `already_exists` to `false`

2. **Determining Update Needs**:
   - When an event matches an existing candidate, assess if the extracted information is more specific or precise
   - Set `needs_update` to `true` if the extracted information provides:
     - More precise date/time (e.g., exact date vs. just a quarter)
     - Additional location information not in the candidate
     - More detailed description
     - Higher confidence or confirmation status
   - If no updates are needed, set `needs_update` to `false`

3. **Information Precision Assessment**:
   - Consider date specificity: "March 15, 2024" is more precise than "Q1 2024" or "March 2024"
   - Consider time specificity: "10:00 AM ET" is more precise than just a date
   - For time zones, prefer full dates with explicit time zones over partial information
   - For descriptions, prefer longer, more detailed descriptions that add new information
   - For locations, prefer specific venues over general locations

4. **Handling Conflicting Information**:
   - If new information conflicts with existing data, assess which is likely more accurate
   - Consider source reliability, specificity, and recency
   - Prefer information that creates a more coherent overall event representation
   - When in doubt about conflicting dates, include the discrepancy in the description

### Date Handling
1. Extract the original date text exactly as mentioned in the source
2. For dates with specific time, include both date and time
3. For recurring events, focus on the next occurrence
4. For quarter references (e.g., "Q2 2023"), capture the quarter period
5. For relative dates ("next week", "in two months"), include the contextual reference

### Date Format Standardization
When extracting events, standardize the date representation:

1. **Primary date and time**: Always provide the main date and time in ISO-8601 format (YYYY-MM-DDThh:mm:ssZ) in the "date" field of your output.
   - Example: "2024-03-26T10:00:00-05:00" for March 26, 2024, at 10am EST.

2. **Original date text**: Always preserve the complete original date text exactly as found in the source in the "original_date_text" field.
   - Example: "March 26, Wednesday | 10am EST - 11am CL/BRT - 2pm UK"

3. **Multi-timezone handling**: For events with multiple time zones, use the primary time zone (usually the first mentioned or event location's time zone) for the standardized date field.
   - Example: For "10am EST - 11am CL/BRT - 2pm UK", use "2024-03-26T10:00:00-05:00" (EST)

4. **Handling incomplete dates**:
   - If only time is missing: Use 00:00:00 (midnight) as the default time
   - If only day is missing: Use the 1st day of the month
   - If only month/year is provided (e.g., "March 2024"): Use YYYY-MM-01T00:00:00Z format
   - For quarter references (e.g., "Q2 2024"): Use the first day of the quarter (2024-04-01T00:00:00Z for Q2)
   - For fiscal quarters/years: Map to the corresponding standard calendar period

5. **Timezone standardization**:
   - Always include the timezone offset in the ISO date when timezone information is available
   - Use Z for UTC/GMT when no specific timezone is mentioned
   - Map common timezone abbreviations to their standard offsets (EST: -05:00, PST: -08:00, etc.)

### Examples

#### Example 1: Earnings Call with Date Update
Input:
```
Financial News Daily - March 3, 2023
Alphabet Inc. (GOOGL) has scheduled its quarterly earnings call for Q1 2023. The call will take place sometime in mid-April 2023, where executives will discuss the company's financial performance and business highlights.
```

Candidates:
```json
[
    {
        "id": "event-123",
        "name": "Alphabet Inc. Q1 2023 Earnings Call",
        "description": "Quarterly earnings call to discuss financial results",
        "date": null,
        "original_date_text": "Q1 2023",
        "location": "Virtual",
        "event_type": "earnings_call",
        "companies_ids": ["alphabet-id"],
        "confidence": 0.9
    }
]
```

Output:
```json
[
    {
        "id": "event-123",
        "name": "Alphabet Inc. Q1 2023 Earnings Call",
        "description": "Quarterly earnings call where executives will discuss the company's financial performance and business highlights",
        "date": "2023-04-15T00:00:00Z",
        "original_date_text": "mid-April 2023",
        "location": "Virtual",
        "event_type": "earnings_call",
        "companies": ["Alphabet Inc.", "GOOGL"],
        "confirmed": true,
        "confidence": 0.95,
        "already_exists": true,
        "needs_update": true
    }
]
```

#### Example 2: Conference with Time Update
Input:
```
Market Watch - May 15, 2023
Tesla (TSLA) CEO Elon Musk will be presenting at the upcoming Annual Automotive Innovation Conference on June 10, 2023, at 2:30 PM EST. The event will be held at the Detroit Convention Center where Musk is expected to discuss Tesla's new battery technology.
```

Candidates:
```json
[
    {
        "id": "event-456",
        "name": "Annual Automotive Innovation Conference",
        "description": "Industry conference featuring Tesla presentation",
        "date": "2023-06-10T00:00:00Z",
        "original_date_text": "June 10, 2023",
        "location": "Detroit Convention Center",
        "event_type": "investor_conference",
        "companies_ids": ["tesla-id"],
        "confidence": 0.85
    }
]
```

Output:
```json
[
    {
        "id": "event-456",
        "name": "Annual Automotive Innovation Conference - Tesla Presentation",
        "description": "Elon Musk will present Tesla's new battery technology at the automotive conference",
        "date": "2023-06-10T14:30:00-05:00",
        "original_date_text": "June 10, 2023, at 2:30 PM EST",
        "location": "Detroit Convention Center",
        "event_type": "investor_conference",
        "companies": ["Tesla", "TSLA"],
        "confirmed": true,
        "confidence": 0.95,
        "already_exists": true,
        "needs_update": true
    }
]
```

#### Example 3: New Product Launch
Input:
```
Tech Insider - July 20, 2023
According to sources familiar with the matter, Apple is planning to unveil its new iPhone 15 lineup at a special event in September 2023. The event is expected to showcase the latest features including an upgraded camera system and a USB-C port replacing the Lightning connector.
```

Output:
```json
[
    {
        "name": "Apple iPhone 15 Launch Event",
        "description": "Special event to unveil the new iPhone 15 lineup with upgraded camera system and USB-C port",
        "date": "2023-09-01T00:00:00Z",
        "original_date_text": "September 2023",
        "location": null,
        "event_type": "product_launch",
        "companies": ["Apple"],
        "confirmed": false,
        "confidence": 0.8,
        "already_exists": false,
        "needs_update": false
    }
]
```

#### Example 4: Already Exists, No Updates Needed
Input:
```
Business Wire - April 2, 2023
A reminder that Microsoft will hold its Q3 fiscal year 2023 earnings conference call on April 25, 2023, at 5:30 PM Eastern Time.
```

Candidates:
```json
[
    {
        "id": "event-789",
        "name": "Microsoft Q3 FY2023 Earnings Conference Call",
        "description": "Quarterly earnings call to discuss financial results for Q3 fiscal year 2023",
        "date": "2023-04-25T17:30:00-04:00",
        "original_date_text": "April 25, 2023, at 5:30 PM Eastern Time",
        "location": "Virtual Conference",
        "event_type": "earnings_call",
        "companies_ids": ["microsoft-id"],
        "confidence": 0.95
    }
]
```

Output:
```json
[
    {
        "id": "event-789",
        "name": "Microsoft Q3 FY2023 Earnings Conference Call",
        "description": "Quarterly earnings call to discuss financial results for Q3 fiscal year 2023",
        "date": "2023-04-25T17:30:00-04:00",
        "original_date_text": "April 25, 2023, at 5:30 PM Eastern Time",
        "location": "Virtual Conference",
        "event_type": "earnings_call",
        "companies": ["Microsoft"],
        "confirmed": true,
        "confidence": 0.95,
        "already_exists": true,
        "needs_update": false
    }
]
```

#### Example 5: Multi-Timezone Event
Input:
```
Financial Markets Update - February 10, 2024
Join XYZ Capital for a Global Investment Webinar on March 26, Wednesday | 10am EST - 11am CL/BRT - 2pm UK. The session will feature market insights and investment strategies for the upcoming quarter.
```

Output:
```json
[
    {
        "name": "XYZ Capital Global Investment Webinar",
        "description": "Webinar featuring market insights and investment strategies for the upcoming quarter",
        "date": "2024-03-26T10:00:00-05:00",
        "original_date_text": "March 26, Wednesday | 10am EST - 11am CL/BRT - 2pm UK",
        "location": "Virtual",
        "event_type": "investor_conference",
        "companies": ["XYZ Capital"],
        "confirmed": true,
        "confidence": 0.95,
        "already_exists": false,
        "needs_update": false
    }
]
```

## Important Guidelines
1. Only extract events that are clearly defined with specific details
2. For each event, provide all available information from the input text
3. Do not fabricate or assume details not present in the text
4. Assign accurate confidence scores reflecting the certainty of extraction
5. When multiple events are present, extract each separately
6. Focus on financial and corporate events relevant to investors
7. Properly assess when information updates are needed based on precision
8. Handle date updates carefully, especially when time zones are involved
9. Consider company name variations when matching events
10. For events with partial information, extract what's available and assign appropriate confidence scores 